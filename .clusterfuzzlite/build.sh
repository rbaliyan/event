#!/bin/bash -eu
compile_native_go_fuzzer github.com/rbaliyan/event/v3/payload FuzzJSONCodecDecode fuzz_json_codec_decode
compile_native_go_fuzzer github.com/rbaliyan/event/v3/transport/codec FuzzJSONTransportCodecDecode fuzz_json_transport_codec_decode
compile_native_go_fuzzer github.com/rbaliyan/event/v3/validation FuzzNewJSONSchemaValidator fuzz_new_json_schema_validator
compile_native_go_fuzzer github.com/rbaliyan/event/v3/validation FuzzJSONSchemaValidate fuzz_json_schema_validate
compile_native_go_fuzzer github.com/rbaliyan/event/v3/schema FuzzDecodeEnvelope fuzz_decode_envelope
compile_native_go_fuzzer github.com/rbaliyan/event/v3/schema FuzzJSONSchemaValidate fuzz_json_schema_validate_schema
compile_native_go_fuzzer github.com/rbaliyan/event/v3/schema FuzzFieldMapperUpcast fuzz_field_mapper_upcast
# MsgPack fuzz targets excluded from ClusterFuzzLite: the vmihailenco/msgpack/v5
# library under ASan instrumentation exceeds the 2.5GB RSS limit. These targets
# are still available for local fuzzing via: go test -fuzz=FuzzMsgPack ./payload/ ./transport/codec/
