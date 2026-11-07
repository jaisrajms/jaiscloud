# shellcheck shell=bash
#
# Case: tftest — Terraform-native assertions (`terraform test`) for the
# attributes the provider exposes after read-back.
#
# The committed module is lifecycle-tftest/main.tf; its lifecycle.tftest.hcl
# applies a bucket, topic/subscription and key ring/key and asserts computed
# ids, self links and nested objects. The shell cases assert the server-side
# fields the provider hides (metageneration, locationType, primary/destroy
# state); this case is the other half of that split.
#
# The run is `init -> test`, and `terraform test` tears its resources down at the
# end. Cloud KMS keeps the key ring and crypto key (the API cannot delete them),
# so the case uses distinct names and an ephemeral emulator, like the other
# cases.

case_main() {
  local dir="$SCRIPT_DIR/lifecycle-tftest"
  if [[ ! -d "$dir" ]]; then
    echo "SKIP: $dir is missing"
    return 0
  fi

  local log="$WORK/tftest.log"
  if ! "$TF" -chdir="$dir" init -input=false -no-color >"$WORK/tftest-init.log" 2>&1; then
    cat "$WORK/tftest-init.log"
    fail "init ($dir)"
    return 0
  fi
  pass "init ($dir)"

  if "$TF" -chdir="$dir" test -no-color \
    -var "endpoint=$ENDPOINT" -var "project=$PROJECT" >"$log" 2>&1; then
    grep -E '^Success!|passed, .* failed' "$log" || true
    pass "terraform test (provider-visible assertions)"
  else
    cat "$log"
    fail "terraform test (see the log above)"
  fi
}
