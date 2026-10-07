# ===== Build =====
ARTIFACT_VERSION ?= 1.0.0

# Аргументы, пробрасываемые в бинарь:
#   make run ARGS="--id=node1 --port=8001 --peers=http://localhost:8002,http://localhost:8003"
ARGS ?=

# ===== Dependencies =====
deps:
	@echo "Installing dependencies..."
	@go mod download
	@go mod tidy

build: deps
	@echo "Building version $(ARTIFACT_VERSION)..."
	@mkdir -p bin
	@go build -o ./bin/raft-node ./cmd/raft-node

# ===== Run =====
run: build
	@echo "Running: ./bin/raft-node $(ARGS)"
	./bin/raft-node $(ARGS)

# ===== Lint =====
lint:
	@echo "Linting..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@golangci-lint run --tests=false --disable-all --timeout=2m -p error

# ===== Cleanup =====
clean:
	@echo "Cleaning..."
	@rm -rf bin/