#!/bin/bash
#===============================================================================
# rollback-setup.sh
# Script de rollback do ambiente AWS para o Glue Job Streaming Mini-Batch
#
# Uso:
#   ./scripts/rollback-setup.sh                    # Usa valores default
#   ./scripts/rollback-setup.sh -e dev              # Ambiente específico
#   ./scripts/rollback-setup.sh -e dev --cleanup-s3 # Limpa buckets S3 também
#
# Dependências:
#   - Terraform >= 1.5
#   - AWS CLI >= 2.0
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

# ─── Parse de argumentos ─────────────────────────────────────────────────────
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -e|--environment)
                ENV="$2"
                shift 2
                ;;
            -h|--help)
                echo "Uso: $0 [-e env]"
                echo "  -e, --environment   Ambiente (dev, staging, prod)  [default: dev]"
                exit 0
                ;;
            *)
                log_error "Argumento desconhecido: $1"
                exit 1
                ;;
        esac
    done
}

# ─── Confirmação ─────────────────────────────────────────────────────────────
confirm_rollback() {
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo -e "${RED}  ATENÇÃO: Rollback do ambiente '${ENV}'${NC}"
    echo "  Account: ${ACCOUNT_ID}"
    echo "  Região:  ${REGION}"
    echo "  ⚠️  Artefatos no S3 não são removidos por este script."
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
    read -p "Deseja continuar? (s/N): " confirm
    if [[ ! "$confirm" =~ ^[sS]$ ]]; then
        log_info "Rollback cancelado."
        exit 0
    fi
}

# ─── Destroy Terraform ──────────────────────────────────────────────────────
destroy_terraform() {
    log_info "Destruindo recursos Terraform do ambiente '${ENV}'..."
    cd "${TERRAFORM_DIR}"

    if [ -d ".terraform" ]; then
        terraform workspace select "${ENV}" 2>/dev/null || terraform workspace new "${ENV}"
        terraform destroy \
            -var="environment=${ENV}" \
            -auto-approve
        log_ok "Recursos Terraform destruídos"
    else
        log_warn "Terraform não inicializado. Execute 'terraform init' primeiro."
    fi
}

# ─── Limpeza de buckets S3 (removido) ───────────────────────────────────────
# A limpeza dos buckets S3 foi removida deste script para evitar deleção
# acidental de dados. Os artefatos (scripts e configs) são gerenciados pelo
# Terraform via null_resource.upload_artifacts. Para remover arquivos do S3,
# utilize o AWS Console ou AWS CLI manualmente.
cleanup_s3() {
    log_info "Limpeza de buckets S3 não executada por este script."
    log_info "Para remover artefatos, use: aws s3 rm s3://${WORKSPACE_BUCKET}/ --recursive"
}

# ─── Main ────────────────────────────────────────────────────────────────────
main() {
    parse_args "$@"
    confirm_rollback

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  Rollback - Glue Streaming Mini-Batch DMS CDC"
    echo "  Ambiente: ${ENV} | Account: ${ACCOUNT_ID}"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""

    destroy_terraform

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo -e "${GREEN}  Rollback concluído com sucesso!${NC}"
    echo "  Recursos do ambiente '${ENV}' removidos."
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
}

main "$@"
