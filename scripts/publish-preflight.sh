#!/usr/bin/env bash
# Publish footgun checks for Team-Sneakymouse Paper plugins (EmptyPlugin template).
# Usage: scripts/publish-preflight.sh [plugin-root]
# Exit 0 when clean; non-zero lists every failure.
set -euo pipefail

ROOT="$(cd "${1:-.}" && pwd)"
cd "$ROOT"

fail=0
err() {
  printf 'publish-preflight: %s\n' "$*" >&2
  fail=1
}

if [[ ! -f gradlew ]]; then
  err "missing ./gradlew"
else
  mode="$(git ls-files -s gradlew 2>/dev/null | awk '{print $1}')"
  if [[ -z "$mode" ]]; then
    err "gradlew is not tracked by git"
  elif [[ "$mode" != "100755" ]]; then
    err "gradlew git mode is $mode (want 100755). Fix: git update-index --chmod=+x gradlew"
  fi
  if [[ ! -x gradlew ]]; then
    err "gradlew is not executable on disk"
  fi
fi

wrapper_jar="gradle/wrapper/gradle-wrapper.jar"
if [[ ! -f "$wrapper_jar" ]]; then
  err "missing $wrapper_jar"
elif ! git ls-files --error-unmatch "$wrapper_jar" >/dev/null 2>&1; then
  err "$wrapper_jar exists but is not tracked (often blocked by *.jar in .gitignore)"
fi

if [[ -f .gitignore ]] && grep -qE '^\s*\*\.jar\s*$' .gitignore; then
  if ! grep -qE '^\s*!gradle/wrapper/gradle-wrapper\.jar\s*$|^\s*!gradle-wrapper\.jar\s*$' .gitignore; then
    err ".gitignore ignores *.jar without un-ignoring gradle-wrapper.jar"
  fi
fi

publish_yml=".github/workflows/publish.yml"
if [[ ! -f "$publish_yml" ]]; then
  err "missing $publish_yml"
else
  if ! grep -q 'workflow_dispatch' "$publish_yml"; then
    err "$publish_yml missing workflow_dispatch (needed to re-run after secret fixes)"
  fi
  if ! grep -q 'publishMavenPublicationToSneakyrpRepository' "$publish_yml"; then
    err "$publish_yml missing publishMavenPublicationToSneakyrpRepository"
  fi
fi

# Team-Sneakymouse sibling / libs bypasses (third-party libs like CMI are out of scope).
while IFS= read -r -d '' f; do
  while IFS= read -r line; do
    err "$f Team-Sneakymouse files() bypass (use maven.sneakyrp.com): $line"
  done < <(grep -nE 'files\([^)]*(\.\./Sneaky|libs/Sneaky)' "$f" || true)
done < <(find . -path ./build -prune -o -path './.gradle' -prune -o \
  \( -name 'build.gradle' -o -name 'build.gradle.kts' \) -print0)

if [[ "$fail" -ne 0 ]]; then
  exit 1
fi
printf 'publish-preflight: ok (%s)\n' "$ROOT"
