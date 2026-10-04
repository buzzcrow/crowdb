#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
engine=$1
shift
case "$engine" in
    spark) main=SparkAcceptance ;;
    flink) main=FlinkAcceptance ;;
    *) echo 'Expected spark or flink' >&2; exit 2 ;;
esac
project="$PIXI_PROJECT_ROOT/container/single-node-container/tests/ecosystem/$engine"
classpath="$project/target/ecosystem-classpath.txt"
mvn --batch-mode --no-transfer-progress -f "$project/pom.xml" compile \
    org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
    "-Dmdep.outputFile=$classpath" -DincludeScope=runtime
# Match Flink's Java 17 module access. A standalone JVM gives asynchronous
# JobMaster deserialization the real application classpath and owns its lifetime.
exports=(java.base/sun.net.util java.rmi/sun.rmi.registry
    jdk.compiler/com.sun.tools.javac.api jdk.compiler/com.sun.tools.javac.file
    jdk.compiler/com.sun.tools.javac.parser jdk.compiler/com.sun.tools.javac.tree
    jdk.compiler/com.sun.tools.javac.util java.security.jgss/sun.security.krb5)
opens=(java.lang java.net java.io java.nio sun.nio.ch java.lang.reflect
    java.text java.time java.util java.util.concurrent java.util.concurrent.atomic
    java.util.concurrent.locks)
options=(-Xmx2g)
for package in "${exports[@]}"; do options+=("--add-exports=$package=ALL-UNNAMED"); done
for package in "${opens[@]}"; do options+=("--add-opens=java.base/$package=ALL-UNNAMED"); done
exec java "${options[@]}" -cp "$(cat "$classpath"):$project/target/classes" "$main" "$@"
