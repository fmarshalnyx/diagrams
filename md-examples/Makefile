# Local Docker Desktop Kubernetes deployment (design §11). See deploy/local/*.sh for what each
# step actually does; this is just the documented entry point.

.PHONY: local-up local-smoke local-down local-failover-drill

local-up:
	deploy/local/00-preflight.sh
	deploy/local/10-build.sh
	deploy/local/20-install-nats.sh
	deploy/local/30-deploy.sh

local-smoke:
	deploy/local/40-smoke-test.sh

local-failover-drill:
	deploy/local/50-failover-drill.sh

local-down:
	deploy/local/90-teardown.sh
