#!/usr/bin/env bash
#===============================================================================
# deploy.sh
# Provision the AWS environment for the Glue CDC pipeline.
#
# Usage:
#   ./ci-cd/deploy.sh                 # default environment (prod)
#   ./ci-cd/deploy.sh -e dev          # specific environment
#   ./ci-cd/deploy.sh -e prod -r us-west-2
#
# Requirements:
#   - Terraform >= 1.5
#   - AWS CLI >= 2.0
#   - jq
#===============================================================================

set -euo pipefail

ENV="prod"
REGION="${AWS_REGION:-us-east-1}"
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
Usage: $0 [-e ENV] [-r REGION]

Options:
  -e, --environment ENV   Terraform workspace / environment (default: prod)
  -r, --region REGION     AWS region (default: \$AWS_REGION or us-east-1)
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
            -r|--region)
                REGION="${2:?Missing region value}"
                shift 2
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

check_prerequisites() {
    log_info "Checking prerequisites..."

    command -v terraform >/dev/null 2>&1 || {
        log_error "Terraform not found. Install from https://developer.hashicorp.com/terraform/downloads"
        exit 1
    }
    log_ok "Terraform $(terraform --version | head -1)"

    command -v aws >/dev/null 2>&1 || {
        log_error "AWS CLI not found. Install from https://aws.amazon.com/cli/"
        exit 1
    }
    log_ok "AWS CLI $(aws --version 2>&1 | cut -d' ' -f1)"

    command -v jq >/dev/null 2>&1 || {
        log_error "jq not found. Install with: sudo apt-get install jq (or brew install jq)"
        exit 1
    }
    log_ok "jq $(jq --version)"

    aws sts get-caller-identity >/dev/null 2>&1 || {
        log_error "AWS credentials not configured. Run 'aws configure' first."
        exit 1
    }
    log_ok "AWS credentials valid"
}

terraform_init() {
    log_info "Initializing Terraform in ${TERRAFORM_DIR}..."
    (cd "${TERRAFORM_DIR}" && terraform init)
    log_ok "Terraform init completed"
}

select_workspace() {
    log_info "Selecting Terraform workspace '${ENV}'..."
    (cd "${TERRAFORM_DIR}" && {
        terraform workspace list 2>/dev/null | grep -q "^\\*\\| ${ENV}$" \
            && terraform workspace select "${ENV}" 2>/dev/null \
            || terraform workspace new "${ENV}"
    })
    log_ok "Workspace '${ENV}' active"
}

terraform_apply() {
    log_info "Applying Terraform for environment '${ENV}'..."
    (cd "${TERRAFORM_DIR}" && terraform apply -var="environment=${ENV}" -auto-approve)
    log_ok "Terraform apply completed"
}

main() {
    parse_args "$@"

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  Deploy - Glue CDC Pipeline"
    echo "  Environment: ${ENV} | Region: ${REGION}"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""

    check_prerequisites
    terraform_init
    select_workspace
    terraform_apply

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo -e "${GREEN}  Deploy completed successfully!${NC}"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
}

main "$@"