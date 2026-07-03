package com.gcm.md.models.bridge;

import com.gcm.md.models.json.JsonTranslator;
import com.gcm.md.models.json.translators.GcmMdTranslators;
import com.gcm.md.models.sbe.MessageHeaderDecoder;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.UnsafeBuffer;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * SBE-to-JSON bridge.
 *
 * Subscribes to {IN_PREFIX}.&gt; (default TICKS_SBE.&gt;), decodes the SBE message header,
 * looks up the generated translator by templateId, writes the JSON bytes and republishes
 * on the same subject with the prefix swapped to {OUT_PREFIX} (default TICKS_JSON), e.g.
 *
 *   TICKS_SBE.QuoteUpdate.EURUSD  -&gt;  TICKS_JSON.QuoteUpdate.EURUSD
 *
 * Threading: a single NATS dispatcher delivers messages on one thread, so the decoder
 * flyweights, translators and buffers held by this handler are thread-confined. To scale,
 * run multiple bridge instances in a queue group or create one handler+dispatcher pair
 * per subject partition - never share a handler across dispatchers.
 *
 * Trust model: the SBE header's templateId is authoritative for decoding. The subject's
 * message-name segment is only a sanity check - on mismatch the message is dropped and
 * logged rather than decoded as the subject-implied type.
 *
 * Configuration via environment variables:
 *   NATS_URL   (default nats://localhost:4223)
 *   IN_PREFIX  (default TICKS_SBE)
 *   OUT_PREFIX (default TICKS_JSON)
 */
public final class BridgeMain implements MessageHandler
{
    private static final System.Logger LOG = System.getLogger(BridgeMain.class.getName());

    private final Connection connection;
    private final String inPrefix;
    private final String outPrefix;

    // Thread-confined to the single dispatcher thread.
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final UnsafeBuffer inBuffer = new UnsafeBuffer(new byte[0]);
    private final ExpandableArrayBuffer outBuffer = new ExpandableArrayBuffer(4096);
    private final Int2ObjectHashMap<JsonTranslator> translators = GcmMdTranslators.create();
    private final IntHashSet warnedUnknownTemplates = new IntHashSet();

    private BridgeMain(final Connection connection, final String inPrefix, final String outPrefix)
    {
        this.connection = connection;
        this.inPrefix = inPrefix;
        this.outPrefix = outPrefix;
    }

    @Override
    public void onMessage(final Message msg)
    {
        try
        {
            final byte[] data = msg.getData();
            if (data == null || data.length < MessageHeaderDecoder.ENCODED_LENGTH)
            {
                LOG.log(System.Logger.Level.WARNING,
                    "dropping runt frame on {0} ({1} bytes)", msg.getSubject(), data == null ? 0 : data.length);
                return;
            }

            inBuffer.wrap(data);
            header.wrap(inBuffer, 0);
            final int templateId = header.templateId();

            final JsonTranslator translator = translators.get(templateId);
            if (translator == null)
            {
                if (warnedUnknownTemplates.add(templateId))
                {
                    LOG.log(System.Logger.Level.WARNING,
                        "no translator for templateId {0} (first seen on {1}); dropping this and further frames of this type",
                        templateId, msg.getSubject());
                }
                return;
            }

            final String subject = msg.getSubject();
            final String subjectMessageName = messageNameSegment(subject);
            if (subjectMessageName != null && !translator.messageName().equals(subjectMessageName))
            {
                LOG.log(System.Logger.Level.WARNING,
                    "subject/message mismatch: subject {0} vs templateId {1} ({2}); dropping",
                    subject, templateId, translator.messageName());
                return;
            }

            final int length = translator.translate(
                inBuffer, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version(), outBuffer);

            final byte[] json = new byte[length];
            outBuffer.getBytes(0, json);
            connection.publish(outPrefix + subject.substring(inPrefix.length()), json);
        }
        catch (final Exception e)
        {
            LOG.log(System.Logger.Level.ERROR, "failed to translate frame on " + msg.getSubject(), e);
        }
    }

    /** Second token of the subject, e.g. "QuoteUpdate" in TICKS_SBE.QuoteUpdate.EURUSD; null if absent. */
    private static String messageNameSegment(final String subject)
    {
        final int first = subject.indexOf('.');
        if (first < 0)
        {
            return null;
        }
        final int second = subject.indexOf('.', first + 1);
        return second < 0 ? subject.substring(first + 1) : subject.substring(first + 1, second);
    }

    public static void main(final String[] args) throws Exception
    {
        final String natsUrl = env("NATS_URL", "nats://localhost:4223");
        final String inPrefix = env("IN_PREFIX", "TICKS_SBE");
        final String outPrefix = env("OUT_PREFIX", "TICKS_JSON");

        final Options options = new Options.Builder()
            .server(natsUrl)
            .connectionName("gcm-md-models-json-bridge")
            .maxReconnects(-1)
            .build();

        final Connection connection = Nats.connect(options);
        LOG.log(System.Logger.Level.INFO, "connected to {0}; bridging {1}.> -> {2}.*", natsUrl, inPrefix, outPrefix);

        final BridgeMain bridge = new BridgeMain(connection, inPrefix, outPrefix);
        final Dispatcher dispatcher = connection.createDispatcher(bridge);
        dispatcher.subscribe(inPrefix + ".>");

        final CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            try
            {
                LOG.log(System.Logger.Level.INFO, "draining and closing NATS connection");
                connection.drain(Duration.ofSeconds(5)).get();
            }
            catch (final Exception e)
            {
                LOG.log(System.Logger.Level.WARNING, "error during drain", e);
            }
            finally
            {
                shutdown.countDown();
            }
        }, "bridge-shutdown"));

        shutdown.await();
    }

    private static String env(final String key, final String defaultValue)
    {
        final String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
