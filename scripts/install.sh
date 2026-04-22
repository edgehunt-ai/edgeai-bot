#!/usr/bin/env bash
set -e

REPO="edgehunt-ai/edgeai-bot"
BIN_NAME="edgeai"
INSTALL_DIR="${HOME}/.edgeai/bin"
TEMP_DIR=$(mktemp -d)

cleanup() {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

detect_shell_rc() {
    local shell_name
    shell_name="$(basename "${SHELL:-}")"

    case "$shell_name" in
        zsh) echo "${HOME}/.zshrc" ;;
        bash) echo "${HOME}/.bashrc" ;;
        *)
            if [ -f "${HOME}/.zshrc" ]; then
                echo "${HOME}/.zshrc"
            else
                echo "${HOME}/.bashrc"
            fi
            ;;
    esac
}

ensure_path_in_shell_rc() {
    local rc_file
    local path_line='export PATH="${HOME}/.edgeai/bin:$PATH"'

    rc_file="$(detect_shell_rc)"
    mkdir -p "$(dirname "$rc_file")"
    touch "$rc_file"

    if grep -Fqs "$path_line" "$rc_file"; then
        echo "PATH already configured in ${rc_file}"
        return 0
    fi

    {
        echo ""
        echo "# Added by edgeai installer"
        echo "$path_line"
    } >> "$rc_file"

    echo "Added ${INSTALL_DIR} to PATH in ${rc_file}"
}

detect_platform() {
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')
    local arch=$(uname -m)

    case "$arch" in
        x86_64) arch="x86_64" ;;
        aarch64|arm64) arch="aarch64" ;;
        *) echo "Unsupported architecture: $arch" >&2; exit 1 ;;
    esac

    case "$os" in
        linux) os="linux" ;;
        darwin) os="macos" ;;
        *) echo "Unsupported OS: $os" >&2; exit 1 ;;
    esac

    echo "${os}-${arch}"
}

download_release() {
    local version="$1"
    local platform="$2"
    local url="https://github.com/${REPO}/releases/download/${version}/${BIN_NAME}-${platform}.tar.gz"
    local extracted_bin

    echo "Downloading ${BIN_NAME} ${version} for ${platform}..."
    if ! curl -fsSL "$url" -o "${TEMP_DIR}/${BIN_NAME}.tar.gz"; then
        echo "Failed to download from GitHub releases, falling back to source build..." >&2
        return 1
    fi

    mkdir -p "$INSTALL_DIR"
    tar -xzf "${TEMP_DIR}/${BIN_NAME}.tar.gz" -C "$TEMP_DIR"
    extracted_bin=$(find "$TEMP_DIR" -type f -name "$BIN_NAME" -perm -u+x | head -n1)

    if [ -z "$extracted_bin" ]; then
        echo "Failed to find ${BIN_NAME} in downloaded archive" >&2
        return 1
    fi

    mv "$extracted_bin" "${INSTALL_DIR}/${BIN_NAME}"
    chmod +x "${INSTALL_DIR}/${BIN_NAME}"
    echo "Installed to ${INSTALL_DIR}/${BIN_NAME}"
}

build_from_source() {
    echo "Building ${BIN_NAME} from source..."
    if ! command -v cargo &> /dev/null; then
        echo "Cargo not found. Please install Rust: https://rustup.rs" >&2
        exit 1
    fi

    # When run via pipe (curl | bash), BASH_SOURCE[0] is empty or 'bash',
    # so there is no local source tree to build from.
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-}")" 2>/dev/null && pwd)"
    local repo_dir="${script_dir}/.."

    if [ ! -f "${repo_dir}/Cargo.toml" ]; then
        echo "No local source found. Clone the repo and run the script directly:" >&2
        echo "  git clone https://github.com/${REPO}.git" >&2
        echo "  cd edgeai && bash scripts/install.sh" >&2
        exit 1
    fi

    cd "$repo_dir"
    cargo build --release --quiet
    mkdir -p "$INSTALL_DIR"
    cp "target/release/${BIN_NAME}" "${INSTALL_DIR}/${BIN_NAME}"
    echo "Installed to ${INSTALL_DIR}/${BIN_NAME}"
}

get_installed_version() {
    if [ -x "${INSTALL_DIR}/${BIN_NAME}" ]; then
        "${INSTALL_DIR}/${BIN_NAME}" --version 2>/dev/null | awk '{print $2}'
    else
        echo ""
    fi
}

main() {
    local version="${1:-latest}"
    local platform=$(detect_platform)

    if [ "$version" = "latest" ]; then
        version=$(curl -s "https://api.github.com/repos/${REPO}/releases/latest" | grep -o '"tag_name": "[^"]*' | cut -d'"' -f4 || echo "")
    fi

    local installed_version
    installed_version=$(get_installed_version)

    if [ -n "$installed_version" ] && [ -n "$version" ] && [ "$installed_version" = "$version" ]; then
        echo "${BIN_NAME} ${version} is already up to date."
        exit 0
    fi

    mkdir -p "$INSTALL_DIR"

    if [ -n "$version" ] && [ "$version" != "latest" ]; then
        download_release "$version" "$platform" || build_from_source
    else
        build_from_source
    fi

    echo ""
    ensure_path_in_shell_rc

    if [[ ":$PATH:" != *":${INSTALL_DIR}:"* ]]; then
        echo ""
        echo "For the current shell, run:"
        echo "  export PATH=\"\${HOME}/.edgeai/bin:\$PATH\""
    fi
}

main "$@"
