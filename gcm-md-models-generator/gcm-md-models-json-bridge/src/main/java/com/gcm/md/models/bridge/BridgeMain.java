package com.gcm.md.models.bridge;

import com.gcm.md.models.json.JsonTranslator;
import com.gcm.md.models.json.translators.GcmMdTranslators;
import com.usbank.gcm.md.sbe.MessageHeaderDecoder;
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
 * Subscribes to {IN_PREFIX}.&gt; (default tick.sbe.&gt;), decodes the SBE message header,
 * looks up the generated translator by templateId, writes the JSON bytes and republishes
 * on the same subject with the prefix swapped to {OUT_PREFIX} (default tick.json), e.g.
 *
 *   tick.sbe.CME.energy.NGQ2026.delta  -&gt;  tick.json.CME.energy.NGQ2026.delta
 *
 * The subject suffix (source, assetClass, symbol, msgType) is passed through verbatim.
 * The SBE header's templateId is authoritative for decoding - subject tokens are
 * routing metadata only and are never used to choose a decoder.
 *
 * Threading: a single NATS dispatcher delivers messages on one thread, so the decoder
 * flyweights, translators and buffers held by this handler are thread-confined. To scale,
 * run multiple bridge instances in a queue group or create one handler+dispatcher pair
 * per subject partition - never share a handler across dispatchers.
 *
 * Configuration via environment variables:
 *   NATS_URL   (default nats://localhost:4222)
 *   IN_PREFIX  (default tick.sbe)
 *   OUT_PREFIX (default tick.json)
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

            final int length = translator.translate(
                inBuffer, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version(), outBuffer);

            final byte[] json = new byte[length];
            outBuffer.getBytes(0, json);
            connection.publish(outPrefix + msg.getSubject().substring(inPrefix.length()), json);
        }
        catch (final Exception e)
        {
            LOG.log(System.Logger.Level.ERROR, "failed to translate frame on " + msg.getSubject(), e);
        }
    }

    public static void main(final String[] args) throws Exception
    {
        final String natsUrl = env("NATS_URL", "nats://localhost:4222");
        final String inPrefix = env("IN_PREFIX", "tick.sbe");
        final String outPrefix = env("OUT_PREFIX", "tick.json");

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
