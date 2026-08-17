#!/usr/bin/env bash
#===============================================================================
# rollback.sh
# Destroy the AWS environment for the Glue DMS CDC pipeline.
#
# Usage:
#   ./ci-cd/rollback.sh                # default environment (prod)
#   ./ci-cd/rollback.sh -e dev         # specific environment
#   ./ci-cd/rollback.sh -e dev --yes   # skip confirmation prompt
#
# Requirements:
#   - Terraform >= 1.5
#   - AWS CLI >= 2.0
#===============================================================================

set -euo pipefail

ENV="prod"
REGION="${AWS_REGION:-us-east-1}"
ASSUME_YES=false
TERRAFORM_DIR="$(cd "$(dirname "$0")/../infra" && pwd)"

# ANSI colors
readonly RED=$'\033[0;31m'
readonly GREEN=$'\033[0;32m'
readonly YELLOW=$'\033[1;33m'
readonly CYAN=$'\033[0;36m'
readonly NC=$'\033[0m'

log_info()  { echo -e "${CYAN}[INFO]${NC}  $1"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}    $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

usage() {
    cat <<EOF
Usage: $0 [-e ENV] [--yes]

Options:
  -e, --environment ENV   Terraform workspace / environment (default: prod)
  -y, --yes               Skip confirmation prompt
  -h, --help              Show this help message
EOF
    exit 0
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -e|--environment)
                ENV="${2:?Missing environment value}"
                shift 2
                ;;
            -y|--yes)
                ASSUME_YES=true
                shift
                ;;
            -h|--help)
                usage
                ;;
            *)
                log_error "Unknown argument: $1"
                usage
                ;;
        esac
    done
}

confirm_rollback() {
    if [[ "$ASSUME_YES" == "true" ]]; then
        return
    fi

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo -e "${RED}  ATTENTION: This will destroy environment '${ENV}'${NC}"
    echo "  Region: ${REGION}"
    echo "  S3 artifacts are NOT removed by this script."
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
    read -r -p "Continue? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[yY]$ ]]; then
        log_info "Rollback cancelled."
        exit 0
    fi
}

terraform_destroy() {
    log_info "Destroying Terraform resources for environment '${ENV}'..."
    if [[ ! -d "${TERRAFORM_DIR}/.terraform" ]]; then
        log_warn "Terraform not initialized. Run ./ci-cd/deploy.sh first."
        return
    fi

    (cd "${TERRAFORM_DIR}" && {
        terraform workspace list 2>/dev/null | grep -q "^\\*\\| ${ENV}$" \
            && terraform workspace select "${ENV}" 2>/dev/null \
            || terraform workspace new "${ENV}"
        terraform destroy -var="environment=${ENV}" -auto-approve
    })
    log_ok "Terraform resources destroyed"
}

main() {
    parse_args "$@"
    confirm_rollback

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  Rollback - Glue DMS CDC Pipeline"
    echo "  Environment: ${ENV} | Region: ${REGION}"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""

    terraform_destroy

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo -e "${GREEN}  Rollback completed successfully!${NC}"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
}

main "$@"