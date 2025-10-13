#!/bin/bash
#
# install.sh - Installation script for MSC (Multi-Storage Client) FUSE filesystem
#
# Usage: sudo ./install.sh [OPTIONS]
#
# Options:
#   --prefix PREFIX    Installation prefix (default: /usr/local)
#   --bindir DIR       Binary installation directory (default: PREFIX/bin)
#   --sbindir DIR      System binary directory (default: /usr/sbin)
#   --logdir DIR       Log directory (default: /var/log/msc)
#   --uninstall        Uninstall MSC
#   --help             Show this help message
#

set -euo pipefail

# Default values
PREFIX="${PREFIX:-/usr/local}"
BINDIR="${BINDIR:-${PREFIX}/bin}"
SBINDIR="${SBINDIR:-/usr/sbin}"
LOGDIR="${LOGDIR:-/var/log/msc}"
UNINSTALL=0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "$0")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

# Function to show usage
usage() {
    cat << EOF
Usage: sudo $SCRIPT_NAME [OPTIONS]

Install MSC (Multi-Storage Client) FUSE filesystem and mount helpers.

Options:
  --prefix PREFIX    Installation prefix (default: /usr/local)
  --bindir DIR       Binary installation directory (default: PREFIX/bin)
  --sbindir DIR      System binary directory (default: /usr/sbin)
  --logdir DIR       Log directory (default: /var/log/msc)
  --uninstall        Uninstall MSC
  --help             Show this help message

Examples:
  # Install with defaults
  sudo ./install.sh

  # Install to custom location
  sudo ./install.sh --prefix /opt/msc

  # Uninstall
  sudo ./install.sh --uninstall

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --prefix)
            PREFIX="$2"
            BINDIR="${PREFIX}/bin"
            shift 2
            ;;
        --bindir)
            BINDIR="$2"
            shift 2
            ;;
        --sbindir)
            SBINDIR="$2"
            shift 2
            ;;
        --logdir)
            LOGDIR="$2"
            shift 2
            ;;
        --uninstall)
            UNINSTALL=1
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Function to check if running as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run as root (use sudo)"
        exit 1
    fi
}

# Function to check if required files exist
check_files() {
    local missing_files=0
    
    if [[ ! -f "$SCRIPT_DIR/mscp" ]]; then
        log_error "mscp binary not found. Please run 'make build' first."
        missing_files=1
    fi
    
    if [[ ! -f "$SCRIPT_DIR/mount.msc" ]]; then
        log_error "mount.msc not found"
        missing_files=1
    fi
    
    if [[ $missing_files -eq 1 ]]; then
        exit 1
    fi
}

# Function to install files
install_msc() {
    log_info "Installing MSC to $PREFIX"
    
    # Create directories
    log_info "Creating directories..."
    mkdir -p "$BINDIR"
    mkdir -p "$SBINDIR"
    mkdir -p "$LOGDIR"
    
    # Install mscp binary
    log_info "Installing mscp binary to $BINDIR/mscp"
    install -m 755 "$SCRIPT_DIR/mscp" "$BINDIR/mscp"
    
    # Install mount helper
    log_info "Installing mount.msc to $SBINDIR/mount.msc"
    install -m 755 "$SCRIPT_DIR/mount.msc" "$SBINDIR/mount.msc"
    
    # Set log directory permissions
    chmod 755 "$LOGDIR"
    
    log_info "${GREEN}Installation complete!${NC}"
    echo ""
    log_info "Installed files:"
    log_info "  Binary:      $BINDIR/mscp"
    log_info "  Mount:       $SBINDIR/mount.msc"
    log_info "  Log dir:     $LOGDIR"
    echo ""
    log_info "Usage:"
    log_info "  mount -t msc <config_file> <mountpoint>"
    log_info "  umount <mountpoint>"
    echo ""
    log_info "For automatic mounting at boot, add to /etc/fstab:"
    log_info "  /path/to/config.yaml  /mnt/point  msc  defaults,_netdev  0  0"
    echo ""
    log_info "See MOUNT_HELPERS.md for detailed documentation"
}

# Function to uninstall files
uninstall_msc() {
    log_info "Uninstalling MSC from $PREFIX"
    
    local removed=0
    
    # Remove mscp binary
    if [[ -f "$BINDIR/mscp" ]]; then
        log_info "Removing $BINDIR/mscp"
        rm -f "$BINDIR/mscp"
        removed=1
    fi
    
    # Remove mount helper
    if [[ -f "$SBINDIR/mount.msc" ]]; then
        log_info "Removing $SBINDIR/mount.msc"
        rm -f "$SBINDIR/mount.msc"
        removed=1
    fi
    
    # Ask about log directory
    if [[ -d "$LOGDIR" ]]; then
        log_warn "Log directory exists: $LOGDIR"
        read -p "Remove log directory? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            log_info "Removing $LOGDIR"
            rm -rf "$LOGDIR"
        else
            log_info "Keeping log directory: $LOGDIR"
        fi
    fi
    
    if [[ $removed -eq 1 ]]; then
        log_info "${GREEN}Uninstallation complete!${NC}"
    else
        log_warn "No MSC files found to remove"
    fi
}

# Main installation logic
main() {
    check_root
    
    if [[ $UNINSTALL -eq 1 ]]; then
        uninstall_msc
    else
        check_files
        install_msc
    fi
}

# Run main function
main

