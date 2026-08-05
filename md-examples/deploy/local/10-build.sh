#!/usr/bin/env bash
# Design §11: build everything and load images into Docker Desktop's daemon, tagged "local".
#
# Deviates from the design doc's literal `mvn package jib:dockerBuild` command: invoking
# jib:dockerBuild as a bare CLI goal runs it against every module in the reactor, including ones
# with no jib config at all (they fall back to jib's own default base-image guess and fail). The
# image-producing modules (cluster-node, nats-bridge, line-handler-template,
# mock-upstream-source) instead bind jib:dockerBuild to the `package` phase in their own
# pom.xml, so a plain `mvn package` builds their images and leaves every other module unaffected.
set -euo pipefail
cd "$(dirname "$0")/../.."

mvn -T 1C -DskipTests package

echo "Images built:"
docker images --filter reference='gcm-md/*:local' --format '  {{.Repository}}:{{.Tag}}'
