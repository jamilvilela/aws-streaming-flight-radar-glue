#!/bin/bash
#===============================================================================
# setup-env.sh
# Script de setup do ambiente AWS para o Glue Job Streaming Mini-Batch (DMS CDC)
#
# Uso:
#   ./scripts/setup-env.sh                    # Usa valores default
#   ./scripts/setup-env.sh -e dev              # Ambiente específico
#   ./scripts/setup-env.sh -e prod -a 123456  # Ambiente + account ID
#
# Dependências:
#   - Terraform >= 1.5
#   - AWS CLI >= 2.0
#   - jq (para parse de JSON)
#===============================================================================

set -euo pipefail

# ─── Configurações ───────────────────────────────────────────────────────────
ENV="${1:-prod}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "000000000000")
REGION="${AWS_REGION:-us-east-1}"
WORKSPACE_BUCKET="lakehouse-workspace-${ACCOUNT_ID}"
TERRAFORM_DIR="$(cd "$(dirname "$0")/../infra" && pwd)"

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log_info()  { echo -e "${CYAN}[INFO]${NC}  $1"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}    $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ─── Pré-requisitos ──────────────────────────────────────────────────────────
check_prerequisites() {
    log_info "Verificando pré-requisitos..."

    if ! command -v terraform &>/dev/null; then
        log_error "Terraform não encontrado. Instale em: https://developer.hashicorp.com/terraform/downloads"
        exit 1
    fi
    log_ok "Terraform $(terraform --version | head -1)"

    if ! command -v aws &>/dev/null; then
        log_error "AWS CLI não encontrado. Instale em: https://aws.amazon.com/cli/"
        exit 1
    fi
    log_ok "AWS CLI $(aws --version 2>&1 | cut -d' ' -f1)"

    if ! command -v jq &>/dev/null; then
        log_error "jq não encontrado. Instale com: sudo apt-get install jq (ou brew install jq)"
        exit 1
    fi
    log_ok "jq $(jq --version)"

    # Verificar credenciais AWS
    if ! aws sts get-caller-identity &>/dev/null; then
        log_error "Credenciais AWS não configuradas. Execute 'aws configure' primeiro."
        exit 1
    fi
    log_ok "Credenciais AWS válidas (Account: ${ACCOUNT_ID})"
}

# ─── Inicializar Terraform ───────────────────────────────────────────────────
init_terraform() {
    log_info "Inicializando Terraform em ${TERRAFORM_DIR}..."
    cd "${TERRAFORM_DIR}"

    terraform init

    log_ok "Terraform init concluído"
}

# ─── Selecionar/Aplicar Workspace ────────────────────────────────────────────
select_workspace() {
    log_info "Selecionando workspace Terraform '${ENV}'..."
    cd "${TERRAFORM_DIR}"

    if terraform workspace list 2>/dev/null | grep -q "^\\*\\| ${ENV}$"; then
        terraform workspace select "${ENV}" 2>/dev/null || terraform workspace new "${ENV}"
    else
        terraform workspace new "${ENV}"
    fi
    log_ok "Workspace '${ENV}' ativo"
}

# ─── Aplicar Terraform ───────────────────────────────────────────────────────
apply_terraform() {
    log_info "Aplicando Terraform..."
    cd "${TERRAFORM_DIR}"

    terraform apply \
        -var="environment=${ENV}" \
        -auto-approve

    # Os artefatos (scripts Python e configs JSON) são copiados automaticamente
    # pelo resource null_resource.upload_artifacts no módulo infra/main.tf

    log_ok "Terraform apply concluído"
}

# ─── Main ────────────────────────────────────────────────────────────────────
main() {
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  Setup Environment - Glue Streaming Mini-Batch DMS CDC"
    echo "  Ambiente: ${ENV} | Account: ${ACCOUNT_ID} | Região: ${REGION}"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""

    check_prerequisites
    init_terraform
    select_workspace
    apply_terraform

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo -e "${GREEN}  Setup concluído com sucesso!${NC}"
    echo "  Bucket workspace: s3://${WORKSPACE_BUCKET}"
    echo "  Os artefatos foram enviados via Terraform (null_resource.upload_artifacts)"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
}

main
