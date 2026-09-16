.PHONY: test proto

PROTO_DIR ?= ../clusdr/proto
PROTO_DST := proto/clusdr/v1alpha1

proto:
	cp $(PROTO_DIR)/clusdr/v1alpha1/health.proto $(PROTO_DST)/
	cp $(PROTO_DIR)/clusdr/v1alpha1/membership.proto $(PROTO_DST)/
	cp $(PROTO_DIR)/clusdr/v1alpha1/watch.proto $(PROTO_DST)/
	cp $(PROTO_DIR)/clusdr/v1alpha1/events.proto $(PROTO_DST)/
	cp $(PROTO_DIR)/clusdr/v1alpha1/locks.proto $(PROTO_DST)/
	cp $(PROTO_DIR)/clusdr/v1alpha1/leases.proto $(PROTO_DST)/
	python3 scripts/inject_java_proto.py $(PROTO_DST)

test:
	mvn -q test
