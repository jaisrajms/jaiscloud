#!/usr/bin/env bash
#
# toolchain.sh — the demo's JDK, Maven and Helm, user-local and idempotent.
#
# The Dataproc-on-GKE demo needs three tools the emulator itself does not: a
# JDK 21 to compile and run the submitter (../java), Maven to build it, and Helm
# to render/install the chart (../helm). Everything here installs under ~/.local
# — no sudo, no apt source, no system directory — and every step checks first,
# so a machine that already has the tools gets a "nothing to do" run.
#
# Usage:
#   demo/dpgke-lifecycle/scripts/toolchain.sh            install what is missing
#   demo/dpgke-lifecycle/scripts/toolchain.sh --check    report only; changes nothing
#   demo/dpgke-lifecycle/scripts/toolchain.sh --help
#
# `--check` is the mode a reader (or a gate) can run without side effects: it
# prints what is present, what it would install, and exits 0 only when the
# toolchain is already complete. It creates no directory, downloads nothing and
# installs nothing.
#
# A machine that already has `java` 21, `mvn` 3.x and `helm` 3+ needs none of
# this. The demo's own gates (../java/run.sh, ../helm/render.sh,
# ../scripts/e2e-k3d.sh) source ~/.sdkman/bin/sdkman-init.sh themselves when the
# tools are missing from PATH, and skip with a message when they really are
# absent — nothing in the demo requires this script to have been run.
#
# Nothing the script does needs root. The one path that would is documented at
# the bottom (apt) and deliberately not implemented.
#
# Env overrides (all optional):
#   DPGKE_OPT_DIR           install root                     (default ~/.local/opt)
#   DPGKE_BIN_DIR           Helm's install directory         (default ~/.local/bin)
#   DPGKE_JDK_MAJOR         JDK feature release              (default 21)
#   DPGKE_MAVEN_VERSION     Maven release                    (default 3.9.16)
#   DPGKE_HELM_VERSION      Helm release, with the leading v (default v4.3.1)
#   DPGKE_HELM_INSTALLER_URL  upstream installer script      (default get-helm-4)

set -euo pipefail

MODE="install"
case "${1:-}" in
  --check) MODE="check" ;;
  --help | -h)
    # The usage block is the leading comment; print it, stripped of the "# ".
    awk 'NR == 1 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "${BASH_SOURCE[0]}"
    exit 0
    ;;
  "")
    ;;
  *)
    echo "unknown argument: $1 (try --check or --help)" >&2
    exit 2
    ;;
esac

OPT_DIR="${DPGKE_OPT_DIR:-$HOME/.local/opt}"
BIN_DIR="${DPGKE_BIN_DIR:-$HOME/.local/bin}"
JDK_MAJOR="${DPGKE_JDK_MAJOR:-21}"
JDK_LINK="$OPT_DIR/jdk-$JDK_MAJOR"
MAVEN_VERSION="${DPGKE_MAVEN_VERSION:-3.9.16}"
MAVEN_LINK="$OPT_DIR/maven"
HELM_VERSION="${DPGKE_HELM_VERSION:-v4.3.1}"

# Adoptium's API redirects to the newest GA build of the requested feature
# release, so this URL stays correct across patch releases (21.0.13 today).
JDK_URL="${DPGKE_JDK_URL:-https://api.adoptium.net/v3/binary/latest/$JDK_MAJOR/ga/linux/x64/jdk/hotspot/normal/eclipse}"

# archive.apache.org keeps every release; dlcdn.apache.org only serves the
# current one (and drops the previous as soon as a new one ships), so a pinned
# URL that outlives a Maven release has to point at the archive.
MAVEN_URL="${DPGKE_MAVEN_URL:-https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz}"

# §0.1 of the wave plan recorded get-helm-3 (Helm 3.16.4 was current then).
# The chart's goldens (helm/golden/*) and the CI job
# (.github/workflows/gcp-terraform.yml) are pinned to Helm 4.3.1, so this
# installs that release from the get-helm-4 installer rather than whatever
# `helm3-latest-version` happens to serve. The chart is apiVersion v2, so Helm 3
# still renders it; set DPGKE_HELM_INSTALLER_URL to the get-helm-3 script if you
# specifically want Helm 3 (and expect a possible golden diff, which is drift to
# review, not a failure of the chart).
HELM_INSTALLER_URL="${DPGKE_HELM_INSTALLER_URL:-https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-4}"

PROBLEMS=0
ACTIONS=()

say() { printf '%s\n' "$*"; }
plan() {
  ACTIONS+=("$*")
  say "  would: $*"
}
problem() {
  PROBLEMS=$((PROBLEMS + 1))
  say "  $*"
}

# SDKMAN is the same read-only courtesy the demo's gates extend: the harness
# shell is non-interactive and ~/.bashrc returns early for those, so a JDK and
# Maven that SDKMAN installed are often not on PATH. Sourcing it changes nothing
# on disk and installs nothing.
sdkman_init() {
  [[ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]] || return 0
  set +u
  # shellcheck disable=SC1091
  . "$HOME/.sdkman/bin/sdkman-init.sh"
  set -u
}

# major_of_java BIN — the feature release ("21" from 21.0.13, "8" from 1.8.0).
major_of_java() {
  local out version major
  out="$("$1" -version 2>&1 || true)"
  version="$(printf '%s\n' "$out" |
    sed -n '1s/.*version "\([^"]*\)".*/\1/p')"
  case "$version" in
    1.*)
      major="${version#1.}"
      major="${major%%.*}"
      ;;
    "")
      major=""
      ;;
    *)
      major="${version%%.*}"
      ;;
  esac
  printf '%s' "$major"
}

# major_of_maven — "3" from `mvn -v`'s first line.
major_of_maven() {
  "$1" -v 2>/dev/null |
    sed -n '1s/^Apache Maven \([0-9][0-9]*\)\..*/\1/p'
}

# major_of_helm — "4" from `helm version --short` ("v4.3.1+gc0d08d0").
major_of_helm() {
  "$1" version --short 2>/dev/null |
    sed -n 's/^v\([0-9][0-9]*\).*/\1/p'
}

need_curl() {
  command -v curl >/dev/null 2>&1 && return 0
  PROBLEMS=$((PROBLEMS + 1))
  say "  curl is required for the download steps and is not on PATH"
  return 1
}

say "== Dataproc-on-GKE demo toolchain (${MODE} mode)"
say "   install root: $OPT_DIR"
say "   Helm goes to: $BIN_DIR"

# Sourced in both modes: it only changes this shell's environment, so --check
# still mutates nothing, but it reports what the machine actually has instead of
# what a non-interactive PATH happens to expose.
sdkman_init

# ── JDK 21 ───────────────────────────────────────────────────────────────────
# "Correct" means a JDK 21, wherever it came from (SDKMAN, the distro, an
# earlier run of this script). A different major is reported, never replaced: a
# script that silently shadows a working JDK is worse than one that stops and
# says which release the demo needs.
say
say "== java: JDK $JDK_MAJOR"
java_bin=""
if [[ -x "$JDK_LINK/bin/java" ]]; then
  java_bin="$JDK_LINK/bin/java"
elif [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]]; then
  java_bin="$JAVA_HOME/bin/java"
elif command -v java >/dev/null 2>&1; then
  java_bin="$(command -v java)"
fi

if [[ -n "$java_bin" ]]; then
  found_major="$(major_of_java "$java_bin")"
  if [[ "$found_major" == "$JDK_MAJOR" ]]; then
    say "  present: $("$java_bin" -version 2>&1 | sed -n '1p') ($java_bin) — nothing to do"
  elif [[ -z "$found_major" ]]; then
    problem "'$java_bin -version' did not report a version; not touching it"
  else
    problem "found JDK $found_major at $java_bin, but the demo compiles for release $JDK_MAJOR (java/pom.xml)"
    say "     not replacing a working JDK — install JDK $JDK_MAJOR yourself, or point JAVA_HOME at one"
  fi
else
  if [[ "$MODE" == "check" ]]; then
    plan "download Temurin JDK $JDK_MAJOR from the Adoptium API into $OPT_DIR, and link $JDK_LINK"
  else
    need_curl || exit 1
    mkdir -p "$OPT_DIR"
    say "  installing Temurin JDK $JDK_MAJOR into $OPT_DIR"
    curl -fsSL "$JDK_URL" | tar -xz -C "$OPT_DIR"
    # The tarball extracts as jdk-21.0.13+9 (or similar); the link is the stable
    # name the rest of the script and the printed PATH use.
    jdk_dir=""
    for candidate in "$OPT_DIR"/jdk-"$JDK_MAJOR"*; do
      [[ -d "$candidate" ]] || continue
      jdk_dir="$candidate"
      break
    done
    if [[ -z "$jdk_dir" ]]; then
      echo "ERROR: the JDK archive extracted nothing matching $OPT_DIR/jdk-$JDK_MAJOR*" >&2
      exit 1
    fi
    ln -sfn "$jdk_dir" "$JDK_LINK"
    say "  installed: $jdk_dir (-> $JDK_LINK)"
  fi
fi

# ── Maven ────────────────────────────────────────────────────────────────────
# Any Maven 3.x is enough for this pom; the pinned release is only what the
# install path fetches, so a newer Maven that is already present is left alone.
say
say "== mvn: Maven 3"
mvn_bin="$(command -v mvn 2>/dev/null || true)"
if [[ -n "$mvn_bin" ]]; then
  found_major="$(major_of_maven "$mvn_bin")"
  if [[ "$found_major" == "3" ]]; then
    say "  present: $("$mvn_bin" -v 2>/dev/null | sed -n '1p') ($mvn_bin) — nothing to do"
  elif [[ -z "$found_major" ]]; then
    problem "'$mvn_bin -v' did not report an Apache Maven version; not touching it"
  else
    problem "found Maven $found_major at $mvn_bin; the demo builds with Maven 3"
    say "     install Maven 3 yourself, or set PATH to put it first"
  fi
else
  if [[ "$MODE" == "check" ]]; then
    plan "download Maven $MAVEN_VERSION from archive.apache.org into $OPT_DIR, and link $MAVEN_LINK"
  else
    need_curl || exit 1
    mkdir -p "$OPT_DIR"
    say "  installing Maven $MAVEN_VERSION into $OPT_DIR"
    curl -fsSL "$MAVEN_URL" | tar -xz -C "$OPT_DIR"
    if [[ ! -d "$OPT_DIR/apache-maven-$MAVEN_VERSION" ]]; then
      echo "ERROR: the Maven archive did not extract $OPT_DIR/apache-maven-$MAVEN_VERSION" >&2
      exit 1
    fi
    ln -sfn "$OPT_DIR/apache-maven-$MAVEN_VERSION" "$MAVEN_LINK"
    say "  installed: $OPT_DIR/apache-maven-$MAVEN_VERSION (-> $MAVEN_LINK)"
  fi
fi

# ── Helm ─────────────────────────────────────────────────────────────────────
# Helm's installer is the upstream one; USE_SUDO=false is what keeps it out of
# /usr/local/bin, and HELM_INSTALL_DIR is where it copies the binary. It also
# honours DESIRED_VERSION, which makes the release reproducible.
say
say "== helm: 3+"
helm_bin="$(command -v helm 2>/dev/null || true)"
if [[ -z "$helm_bin" && -x "$BIN_DIR/helm" ]]; then
  helm_bin="$BIN_DIR/helm"
fi
if [[ -n "$helm_bin" ]]; then
  found_major="$(major_of_helm "$helm_bin")"
  if [[ -n "$found_major" && "$found_major" -ge 3 ]]; then
    say "  present: Helm $("$helm_bin" version --short) ($helm_bin) — nothing to do"
  elif [[ -z "$found_major" ]]; then
    problem "'$helm_bin version --short' did not report a version; not touching it"
  else
    problem "found Helm $found_major at $helm_bin; the chart needs Helm 3 or newer"
  fi
else
  if [[ "$MODE" == "check" ]]; then
    plan "run the upstream Helm installer with USE_SUDO=false HELM_INSTALL_DIR=$BIN_DIR DESIRED_VERSION=$HELM_VERSION"
  else
    need_curl || exit 1
    mkdir -p "$BIN_DIR"
    say "  installing Helm $HELM_VERSION into $BIN_DIR"
    USE_SUDO=false HELM_INSTALL_DIR="$BIN_DIR" DESIRED_VERSION="$HELM_VERSION" \
      bash -c "$(curl -fsSL "$HELM_INSTALLER_URL")"
  fi
fi

# ── Export and verify ────────────────────────────────────────────────────────
# The script cannot change the caller's shell, so it exports for its own
# verification and prints the two lines to add to ~/.bashrc (or whatever your
# shell reads) once.
if [[ -d "$JDK_LINK" ]]; then
  export JAVA_HOME="$JDK_LINK"
elif [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]]; then
  export JAVA_HOME
fi
export PATH="${JAVA_HOME:+$JAVA_HOME/bin:}$MAVEN_LINK/bin:$BIN_DIR:$PATH"

say
say "== verification"
if [[ "$MODE" == "check" ]]; then
  say "  (check mode: nothing was downloaded and no directory was created)"
  if [[ ${#ACTIONS[@]} -gt 0 ]]; then
    say
    say "  ${#ACTIONS[@]} step(s) would run; re-run without --check to perform them"
  elif [[ "$PROBLEMS" -eq 0 ]]; then
    say "  nothing to do — this machine already has the demo's toolchain"
  fi
  if [[ "$PROBLEMS" -gt 0 ]]; then
    say "  $PROBLEMS item(s) need attention (see above)"
  fi
  [[ "$PROBLEMS" -eq 0 && ${#ACTIONS[@]} -eq 0 ]]
  exit $?
fi

verify_java() {
  local path
  path="$(command -v java 2>/dev/null || true)"
  if [[ -n "$path" && "$(major_of_java "$path")" == "$JDK_MAJOR" ]]; then
    printf '  [PASS] java %s (%s)\n' "$JDK_MAJOR" "$path"
  else
    printf '  [FAIL] no JDK %s java on PATH\n' "$JDK_MAJOR"
    PROBLEMS=$((PROBLEMS + 1))
  fi
}

verify_mvn() {
  local path
  path="$(command -v mvn 2>/dev/null || true)"
  if [[ -n "$path" ]]; then
    printf '  [PASS] mvn (%s)\n' "$path"
  else
    printf '  [FAIL] mvn is not on PATH\n'
    PROBLEMS=$((PROBLEMS + 1))
  fi
}

verify_helm() {
  local path
  path="$(command -v helm 2>/dev/null || true)"
  if [[ -n "$path" ]]; then
    printf '  [PASS] helm %s (%s)\n' "$(helm version --short 2>/dev/null || true)" "$path"
  else
    printf '  [FAIL] helm is not on PATH\n'
    PROBLEMS=$((PROBLEMS + 1))
  fi
}

verify_java
verify_mvn
verify_helm

say
say "  JAVA_HOME=$JAVA_HOME"
say "  java: $(java -version 2>&1 | sed -n '1p' || true)"
say "  mvn:  $(mvn -v 2>/dev/null | sed -n '1p' || true)"
say "  helm: $(helm version --short 2>/dev/null || true)"
say
if [[ -d "$JDK_LINK" || -d "$MAVEN_LINK" || -x "$BIN_DIR/helm" ]]; then
  say "  Persist the exports by appending these two lines to your shell rc:"
  say "    export JAVA_HOME=\"$JAVA_HOME\""
  say "    export PATH=\"\$JAVA_HOME/bin:$MAVEN_LINK/bin:$BIN_DIR:\$PATH\""
else
  say "  Nothing to persist: java, mvn and helm are already reachable from your shell"
  say "  (JAVA_HOME=$JAVA_HOME)."
fi

say
if [[ "$PROBLEMS" -eq 0 ]]; then
  say "== toolchain complete"
else
  say "== $PROBLEMS item(s) still need attention (see above)"
fi
[[ "$PROBLEMS" -eq 0 ]]

# ── Alternatives (documented here; this script runs neither) ─────────────────
#
# SDKMAN — also user-local, no sudo, and the way this repo's own environment
# happens to be set up. It manages the JDK and Maven together:
#
#   curl -s "https://get.sdkman.io" | bash
#   source "$HOME/.sdkman/bin/sdkman-init.sh"
#   sdk update                            # a stale candidate list is the usual "not found"
#   sdk list java | grep -E '21\..*-tem'  # the authority for the exact identifier
#   sdk install java 21.0.13.0+9-tem      # four-part version + build, not 21.0.13-tem
#   sdk install maven 3.9.16
#
# Two traps, both recorded in §0.1 of the wave plan:
#   1. SDKMAN spells Temurin with the four-part version plus the build number, so
#      the Adoptium name 21.0.13 becomes 21.0.13.0+9-tem. "21.0.13-tem" does not
#      exist and `sdk install` fails with "not found". The identifier moves with
#      every patch, so take it from `sdk list java`, never from a document.
#   2. Do not run a bare `sdk install java`: it installs the current default
#      (25.x at the time of writing), which is not the release this demo and its
#      libraries-bom target. Name the version.
#
# Then `source "$HOME/.sdkman/bin/sdkman-init.sh"` and the demo gates find the
# tools (they source it themselves when java/mvn are not on PATH).
#
# apt — needs sudo, so it is not what this script does. Pop!_OS is Ubuntu-based,
# so the Adoptium repo takes the distro codename (jammy on 22.04, noble on
# 24.04). Run these yourself; they are root-level changes:
#
#   sudo apt-get install -y wget apt-transport-https gpg
#   sudo mkdir -p /etc/apt/keyrings
#   wget -qO- https://packages.adoptium.net/artifactory/api/gpg/key/public \
#     | sudo gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg
#   echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb $(. /etc/os-release && echo $VERSION_CODENAME) main" \
#     | sudo tee /etc/apt/sources.list.d/adoptium.list
#   sudo apt-get update && sudo apt-get install -y temurin-21-jdk maven
#
#   curl -fsSL https://baltocdn.com/helm/signing.asc \
#     | gpg --dearmor | sudo tee /usr/share/keyrings/helm.gpg > /dev/null
#   echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/helm.gpg] https://baltocdn.com/helm/stable/debian/ all main" \
#     | sudo tee /etc/apt/sources.list.d/helm-stable-debian.list
#   sudo apt-get update && sudo apt-get install -y helm
