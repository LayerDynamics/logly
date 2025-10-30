#!/bin/bash
# Logly Control Script
# Manage Logly installation, services, and operations

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGLY_DIR="${SCRIPT_DIR}"

# Default configuration
INSTALL_TYPE="user"  # user or system
VENV_DIR="${LOGLY_DIR}/.venv"
CONFIG_DIR="${HOME}/.config/logly"
LOG_DIR="${HOME}/.local/share/logly/logs"
DATA_DIR="${HOME}/.local/share/logly/data"

# Print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
is_root() {
    [ "$(id -u)" -eq 0 ]
}

# Show usage
usage() {
    cat << EOF
Usage: $0 [COMMAND] [OPTIONS]

Logly Control Script - Manage Logly installation and services

COMMANDS:
    install         Install Logly and dependencies
    uninstall       Remove Logly installation
    start           Start Logly services
    stop            Stop Logly services
    restart         Restart Logly services
    status          Show service status
    logs            Show Logly logs
    query           Query for issues and problems
    test            Run tests
    dev             Development mode
    help            Show this help message

OPTIONS:
    --system        Install system-wide (requires root)
    --user          Install for current user (default)
    --dev           Install with development dependencies

EXAMPLES:
    $0 install              # Install for current user
    $0 install --system     # Install system-wide
    $0 install --dev        # Install with dev dependencies
    $0 start                # Start services
    $0 status               # Check status
    $0 logs                 # View logs
    $0 query health         # Check system health
    $0 query security       # Analyze security threats
    $0 query errors         # Review error trends

EOF
}

# Setup Python virtual environment
setup_venv() {
    local install_dev=$1

    if [ ! -d "${VENV_DIR}" ]; then
        print_info "Creating virtual environment..."
        python3 -m venv "${VENV_DIR}"
    fi

    print_info "Activating virtual environment..."
    source "${VENV_DIR}/bin/activate"

    print_info "Upgrading pip..."
    pip install --upgrade pip setuptools wheel

    print_info "Installing Logly..."
    if [ "$install_dev" = "true" ]; then
        print_info "Installing with development dependencies..."
        pip install -r "${LOGLY_DIR}/requirements/requirements.txt"
        pip install -r "${LOGLY_DIR}/requirements/requirements-dev.txt"
    else
        pip install -r "${LOGLY_DIR}/requirements/requirements.txt"
    fi

    # Install in editable mode
    pip install -e "${LOGLY_DIR}"
}

# Install Logly
cmd_install() {
    local install_dev="false"

    # Parse options
    while [[ $# -gt 0 ]]; do
        case $1 in
            --system)
                if ! is_root; then
                    print_error "System install requires root privileges"
                    exit 1
                fi
                INSTALL_TYPE="system"
                CONFIG_DIR="/etc/logly"
                LOG_DIR="/var/log/logly"
                DATA_DIR="/var/lib/logly"
                shift
                ;;
            --user)
                INSTALL_TYPE="user"
                shift
                ;;
            --dev)
                install_dev="true"
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done

    print_info "Installing Logly (${INSTALL_TYPE} mode)..."

    # Check Python version
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is required but not found"
        exit 1
    fi

    python_version=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
    print_info "Found Python ${python_version}"

    # Setup virtual environment
    setup_venv "$install_dev"

    # Create directories
    print_info "Creating directories..."
    mkdir -p "${CONFIG_DIR}"
    mkdir -p "${LOG_DIR}"
    mkdir -p "${DATA_DIR}"

    # Copy default config if it doesn't exist
    if [ ! -f "${CONFIG_DIR}/config.yml" ]; then
        if [ -f "${LOGLY_DIR}/config/config.yml" ]; then
            print_info "Copying default configuration..."
            cp "${LOGLY_DIR}/config/config.yml" "${CONFIG_DIR}/config.yml"
        fi
    fi

    print_info "Installation complete!"
    print_info "Config directory: ${CONFIG_DIR}"
    print_info "Log directory: ${LOG_DIR}"
    print_info "Data directory: ${DATA_DIR}"
    print_info ""
    print_info "To activate the environment: source ${VENV_DIR}/bin/activate"
    print_info "To start Logly: $0 start"
}

# Uninstall Logly
cmd_uninstall() {
    print_warn "Uninstalling Logly..."

    # Stop services first
    cmd_stop

    # Remove virtual environment
    if [ -d "${VENV_DIR}" ]; then
        print_info "Removing virtual environment..."
        rm -rf "${VENV_DIR}"
    fi

    # Ask before removing data
    read -p "Remove configuration and data directories? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        [ -d "${CONFIG_DIR}" ] && rm -rf "${CONFIG_DIR}"
        [ -d "${LOG_DIR}" ] && rm -rf "${LOG_DIR}"
        [ -d "${DATA_DIR}" ] && rm -rf "${DATA_DIR}"
        print_info "Data removed"
    fi

    print_info "Uninstall complete"
}

# Start Logly services
cmd_start() {
    print_info "Starting Logly services..."

    if [ ! -d "${VENV_DIR}" ]; then
        print_error "Logly not installed. Run: $0 install"
        exit 1
    fi

    source "${VENV_DIR}/bin/activate"

    # Check if already running
    if [ -f "${DATA_DIR}/logly.pid" ]; then
        pid=$(cat "${DATA_DIR}/logly.pid")
        if ps -p "$pid" > /dev/null 2>&1; then
            print_warn "Logly is already running (PID: $pid)"
            exit 0
        fi
    fi

    # Start the service
    nohup python3 -m logly.agent \
        --config "${CONFIG_DIR}/config.yml" \
        > "${LOG_DIR}/logly.log" 2>&1 &

    echo $! > "${DATA_DIR}/logly.pid"
    print_info "Logly started (PID: $(cat ${DATA_DIR}/logly.pid))"
}

# Stop Logly services
cmd_stop() {
    print_info "Stopping Logly services..."

    if [ ! -f "${DATA_DIR}/logly.pid" ]; then
        print_warn "Logly is not running"
        exit 0
    fi

    pid=$(cat "${DATA_DIR}/logly.pid")
    if ps -p "$pid" > /dev/null 2>&1; then
        kill "$pid"
        print_info "Logly stopped"
    else
        print_warn "Process not found (stale PID file)"
    fi

    rm -f "${DATA_DIR}/logly.pid"
}

# Restart Logly services
cmd_restart() {
    print_info "Restarting Logly services..."
    cmd_stop
    sleep 2
    cmd_start
}

# Show service status
cmd_status() {
    print_info "Checking Logly status..."

    if [ ! -f "${DATA_DIR}/logly.pid" ]; then
        print_warn "Logly is not running"
        exit 0
    fi

    pid=$(cat "${DATA_DIR}/logly.pid")
    if ps -p "$pid" > /dev/null 2>&1; then
        print_info "Logly is running (PID: $pid)"

        # Show some basic info
        ps -p "$pid" -o pid,vsz,rss,etime,cmd
    else
        print_warn "Logly is not running (stale PID file)"
        rm -f "${DATA_DIR}/logly.pid"
    fi
}

# Show logs
cmd_logs() {
    if [ -f "${LOG_DIR}/logly.log" ]; then
        tail -f "${LOG_DIR}/logly.log"
    else
        print_warn "No log file found at ${LOG_DIR}/logly.log"
    fi
}

# Query for issues and problems
cmd_query() {
    if [ ! -d "${VENV_DIR}" ]; then
        print_error "Logly not installed. Run: $0 install"
        exit 1
    fi

    source "${VENV_DIR}/bin/activate"

    # Pass all arguments to logly query command
    python3 -m logly.cli query "$@"
}

# Run tests
cmd_test() {
    print_info "Running tests..."

    if [ ! -d "${VENV_DIR}" ]; then
        print_error "Virtual environment not found. Run: $0 install --dev"
        exit 1
    fi

    source "${VENV_DIR}/bin/activate"

    cd "${LOGLY_DIR}"
    pytest tests/ -v --cov=logly
}

# Development mode
cmd_dev() {
    print_info "Starting development mode..."

    if [ ! -d "${VENV_DIR}" ]; then
        print_error "Virtual environment not found. Run: $0 install --dev"
        exit 1
    fi

    source "${VENV_DIR}/bin/activate"

    # Run in foreground with debug logging
    python3 -m logly.agent \
        --config "${CONFIG_DIR}/config.yml" \
        --debug
}

# Main command dispatcher
main() {
    if [ $# -eq 0 ]; then
        usage
        exit 0
    fi

    command=$1
    shift

    case $command in
        install)
            cmd_install "$@"
            ;;
        uninstall)
            cmd_uninstall "$@"
            ;;
        start)
            cmd_start "$@"
            ;;
        stop)
            cmd_stop "$@"
            ;;
        restart)
            cmd_restart "$@"
            ;;
        status)
            cmd_status "$@"
            ;;
        logs)
            cmd_logs "$@"
            ;;
        query)
            cmd_query "$@"
            ;;
        test)
            cmd_test "$@"
            ;;
        dev)
            cmd_dev "$@"
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            print_error "Unknown command: $command"
            usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
