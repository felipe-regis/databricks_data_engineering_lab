# 🚀 Databricks Data Engineering Lab

Este repositório contém pipelines de dados modernos construídos na **Databricks Data Intelligence Platform**, seguindo as melhores práticas de Engenharia de Software para Dados, governança com Unity Catalog e orquestração usando **Databricks Asset Bundles (DABs)**.

## 🏗️ Arquitetura e Padrões
*   **Padrão Arquitetural**: Medallion Architecture (Bronze, Silver, Gold).
*   **Deploy e Orquestração**: Databricks Asset Bundles (DABs).
*   **Desenvolvimento Local**: Integração com VSCode, Python Virtual Environments (`.venv`) e testes unitários locais com `pytest`.
*   **Governança**: Unity Catalog (Managed Volumes e Tabelas).

## 🛠️ Pré-requisitos
Antes de executar este projeto localmente, certifique-se de ter instalado:
1. [Python 3.10+](https://www.python.org/downloads/)
2. [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) configurado e autenticado com seu Workspace.
3. Git

## 💻 Configuração do Ambiente Local

Siga as etapas abaixo para clonar o repositório e configurar seu ambiente virtual para desenvolvimento e testes locais.

**1. Clone o repositório:**
```bash
git clone [https://github.com/felipe-regis/databricks_data_engineering_lab.git](https://github.com/felipe-regis/databricks_data_engineering_lab.git)
cd seu-repositorio