# Runtime-only image: build the jar first with `mvn -pl md-sequencer -am install`, then
#   docker build -t gcm-md-sequencer:latest -f md-sequencer/Dockerfile .
# (build context is the repo root so this can COPY the already-built jar out of target/).
#
# The same image is used for every environment (local/dev/uat/prod/prod-dr) — only
# SPRING_PROFILES_ACTIVE (set per k8s overlay, see k8s/README.md) differs.
FROM eclipse-temurin:25-jre-alpine

RUN addgroup -S sequencer && adduser -S sequencer -G sequencer
USER sequencer
WORKDIR /workspace

# md-sequencer-exec.jar (classifier "exec") is the runnable Spring Boot fat jar; the plain
# md-sequencer.jar is the slim artifact other Maven modules depend on and is not runnable on its own.
COPY --chown=sequencer:sequencer md-sequencer/target/md-sequencer-exec.jar app.jar

EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
