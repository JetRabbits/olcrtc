.PHONY: help image image-push image-buildx-push image-buildx-load print-image

IMAGE ?= ghcr.io/jetrabbits/galactic_vpn/galactic-olcrtc
TAG ?= latest

# For Kubernetes nodes we currently target linux/amd64.
PLATFORM ?= linux/amd64

DOCKERFILE ?= Dockerfile
CONTEXT ?= .

help: ## Show available make targets
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_\-]+:.*##/ {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

print-image: ## Print full image ref
	@echo "$(IMAGE):$(TAG)"

image: ## Build image locally with docker
	docker build -t "$(IMAGE):$(TAG)" -f "$(DOCKERFILE)" "$(CONTEXT)"

image-push: ## Push already-built image tag to registry
	docker push "$(IMAGE):$(TAG)"

image-buildx-load: ## Build with buildx for PLATFORM and load into local docker
	docker buildx build --platform "$(PLATFORM)" -t "$(IMAGE):$(TAG)" --load -f "$(DOCKERFILE)" "$(CONTEXT)"

image-buildx-push: ## Build with buildx for PLATFORM and push to registry
	docker buildx build --platform "$(PLATFORM)" -t "$(IMAGE):$(TAG)" --push -f "$(DOCKERFILE)" "$(CONTEXT)"
