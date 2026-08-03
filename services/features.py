"""Catálogo de permissões apresentado na mesma hierarquia do menu principal."""

from copy import deepcopy


def _group(group_id, nome, children):
    return {"id": group_id, "nome": nome, "group": True, "children": children}


FEATURE_ALIASES = {
    "atualizar/analise-receita": "atualizar/receita-anexo10",
}


def normalize_feature_id(feature_id):
    return FEATURE_ALIASES.get(feature_id, feature_id)


def normalize_feature_list(features):
    normalized = []
    seen = set()
    for feature in features or []:
        feature_id = normalize_feature_id(feature)
        if feature_id and feature_id not in seen:
            normalized.append(feature_id)
            seen.add(feature_id)
    return normalized


FEATURES = [
    {"id": "dashboard", "nome": "Início", "locked": True, "children": []},
    {
        "id": "atualizar",
        "nome": "Administrar SPO",
        "children": [
            _group("grupo/personalizar-spo", "Personalizar SPO", [
                {"id": "atualizar/personalizar-spo/temas", "nome": "Personalizar Temas SPO"},
                {"id": "atualizar/personalizar-spo/contrato-visual-protegido", "nome": "Contrato Visual Protegido"},
            ]),
            _group("grupo/governanca-resultados", "Governança e Resultados", [
                {"id": "atualizar/governanca-resultados/mapa", "nome": "Mapa Conceitual"},
                {"id": "atualizar/governanca-resultados/programacao-pta2027", "nome": "Programação PTA 2027"},
                {"id": "atualizar/governanca-resultados/estrategia", "nome": "Estratégia e Cadeia de Valor"},
                {"id": "atualizar/governanca-resultados/eap-politicas", "nome": "EAP e Políticas"},
                {"id": "atualizar/governanca-resultados/entregas-okr", "nome": "Entregas-MT e OKR"},
                {"id": "atualizar/governanca-resultados/processos-riscos-projetos", "nome": "Processos, Riscos e Projetos"},
                {"id": "atualizar/governanca-resultados/matriculas-metas", "nome": "Matrículas e Metas Físicas"},
            ]),
            _group("grupo/pta-programacao", "PTA - Programação", [
                _group("grupo/estrutura-planejamento", "Estrutura do Planejamento", [
                    {"id": "atualizar/estrutura-planejamento/programas", "nome": "Programas"},
                    {"id": "atualizar/estrutura-planejamento/acoes", "nome": "Ações/PAOE"},
                    {"id": "atualizar/estrutura-planejamento/produtos", "nome": "Produtos da Ação"},
                    {"id": "atualizar/estrutura-planejamento/componentes", "nome": "Componentes"},
                    {"id": "atualizar/estrutura-planejamento/componentes-rev", "nome": "Componentes da Revista"},
                    {"id": "atualizar/estrutura-planejamento/vinculos", "nome": "Gerenciar Vínculos"},
                    {"id": "atualizar/estrutura-planejamento/modelos-chave", "nome": "Modelos de Chave"},
                    {"id": "atualizar/estrutura-planejamento/catalogo-chave", "nome": "Catálogo de Chaves"},
                    {"id": "atualizar/estrutura-planejamento/replicar-exercicio", "nome": "Replicar exercício"},
                ]),
                {"id": "atualizar/teto-seduc", "nome": "Teto Financeiro"},
            ]),
            _group("grupo/pta-gerencial-nger", "PTA-Gerencial NGER", [
                {"id": "atualizar/plan20-seduc", "nome": "PLAN-20 FIPLAN"},
                _group("grupo/pta-gerencial-atualizacao", "Atualização", [
                    {"id": "cadastrar/plan_21-nger/meta_fisica", "nome": "Meta Física"},
                    {"id": "cadastrar/plan_21-nger/subacao", "nome": "Subação"},
                    {"id": "cadastrar/plan_21-nger/etapa", "nome": "Etapa"},
                ]),
            ]),
            _group("grupo/atualizar-execucao", "Atualizar Execução Orçamentária", [
                {"id": "atualizar/chave_planejamento_regra", "nome": "Correção de Erros de Chave"},
                {"id": "atualizar/chaves_planejamento_upload", "nome": "Upload anual de chaves de planejamento"},
                {"id": "atualizar/fip613", "nome": "FIP 613"},
                {"id": "atualizar/ped", "nome": "PED"},
                {"id": "atualizar/emp", "nome": "EMP"},
                {"id": "atualizar/receita-anexo10", "nome": "Receita Anexo 10"},
                {"id": "atualizar/est-emp", "nome": "EST EMP"},
                {"id": "atualizar/nob", "nome": "NOB"},
            ]),
        ],
    },
    {"id": "cadastrar/planejamento/programar-pta-loa", "nome": "Programar PTA/LOA", "children": []},
    {
        "id": "cadastrar",
        "nome": "Emitir/Ajustar Dotação",
        "children": [
            {"id": "cadastrar/dotacao", "nome": "Dotação"},
            {"id": "cadastrar/est-dotacao", "nome": "Estorno de Dotação"},
        ],
    },
    {
        "id": "institucional",
        "nome": "Institucional",
        "children": [
            {"id": "institucional/diretrizes", "nome": "Diretrizes e Procedimentos"},
            {"id": "institucional/repositorio", "nome": "Repositório de Arquivos"},
            {"id": "institucional/legislacao", "nome": "Legislação e Normas"},
            {"id": "institucional/parceiros", "nome": "Rede de Parceiros"},
        ],
    },
    {
        "id": "relatorios",
        "nome": "Relatórios",
        "children": [
            _group("grupo/relatorios-planejamento", "Planejamento", [
                {"id": "relatorios/plan20-seduc", "nome": "PLAN20 - SEDUC"},
                {"id": "relatorios/plan21-nger", "nome": "PLAN21_NGER"},
                _group("grupo/relatorios-estrutura", "Estrutura do Planejamento", [
                    {"id": "relatorios/estrutura-planejamento", "nome": "Consultar Estrutura"},
                ]),
            ]),
            _group("grupo/relatorios-execucao", "Execução", [
                {"id": "relatorios/fip613", "nome": "FIP 613"},
                {"id": "relatorios/ped", "nome": "PED"},
                {"id": "relatorios/emp", "nome": "EMP"},
                {"id": "relatorios/est-emp", "nome": "EST EMP"},
                {"id": "relatorios/nob", "nome": "NOB"},
                {"id": "relatorios/receita-anexo10", "nome": "Receita Anexo 10"},
                {"id": "relatorios/dotacao", "nome": "DOTAÇÃO"},
                {"id": "relatorios/est-dotacao", "nome": "ESTORNO DE DOTAÇÃO"},
            ]),
        ],
    },
    {
        "id": "paineis-dashboards",
        "nome": "Painéis/Dashboards",
        "children": [
            _group("grupo/paineis-planejamento", "Planejamento", [
                {"id": "paineis-dashboards/teto-orcamentario", "nome": "Teto Orçamentário"},
            ]),
        ],
    },
    {
        "id": "area-uens",
        "nome": "Área UENs",
        "children": [
            {
                "id": "area-uens/sage",
                "nome": "SAGE",
                "children": [
                    {"id": "area-uens/sage/notas-see", "nome": "Notas SEE"},
                ],
            },
        ],
    },
    {
        "id": "usuarios",
        "nome": "Usuários",
        "children": [
            {"id": "usuarios/cadastrar", "nome": "Cadastrar"},
            {"id": "usuarios/editar", "nome": "Editar"},
            {"id": "usuarios/perfil", "nome": "Perfil"},
            {"id": "usuarios/senha", "nome": "Alterar Senha"},
            {"id": "usuarios/api-acessos", "nome": "API de Acessos"},
        ],
    },
    {"id": "painel", "nome": "Painel - Permissões", "children": []},
    {"id": "logout", "nome": "Sair", "locked": True, "children": []},
]


def flatten_features(features=None):
    """Retorna somente IDs que representam permissões persistíveis."""
    items = FEATURES if features is None else features
    flat = []
    for feature in items:
        if not feature.get("group"):
            flat.append(feature["id"])
        flat.extend(flatten_features(feature.get("children") or []))
    return flat


def build_parent_map(features=None, permission_parent=None):
    """Relaciona cada permissão ao ancestral persistível mais próximo."""
    items = FEATURES if features is None else features
    parent_map = {}
    for feature in items:
        current_parent = permission_parent
        if not feature.get("group"):
            if permission_parent:
                parent_map[feature["id"]] = permission_parent
            current_parent = feature["id"]
        parent_map.update(build_parent_map(feature.get("children") or [], current_parent))
    return parent_map


MENU_META = {
    "dashboard": {"icon": "house"},
    "atualizar": {"icon": "sliders", "css_class": "menu-admin-spo"},
    "grupo/personalizar-spo": {"icon": "palette"},
    "atualizar/personalizar-spo/temas": {"icon": "brush"},
    "atualizar/personalizar-spo/contrato-visual-protegido": {"icon": "shield-lock"},
    "grupo/governanca-resultados": {"icon": "compass"},
    "atualizar/governanca-resultados/mapa": {"icon": "diagram-3"},
    "atualizar/governanca-resultados/programacao-pta2027": {"icon": "map"},
    "atualizar/governanca-resultados/estrategia": {"icon": "bullseye"},
    "atualizar/governanca-resultados/eap-politicas": {"icon": "layers"},
    "atualizar/governanca-resultados/entregas-okr": {"icon": "check2-square"},
    "atualizar/governanca-resultados/processos-riscos-projetos": {"icon": "kanban"},
    "atualizar/governanca-resultados/matriculas-metas": {"icon": "people"},
    "grupo/pta-programacao": {"icon": "diagram-3"},
    "grupo/estrutura-planejamento": {"icon": "diagram-2"},
    "grupo/pta-gerencial-nger": {"icon": "diagram-3-fill"},
    "grupo/pta-gerencial-atualizacao": {"icon": "pencil-square"},
    "grupo/atualizar-execucao": {"icon": "bar-chart-steps"},
    "cadastrar": {"icon": "clipboard-plus"},
    "cadastrar/dotacao": {"icon": "clipboard-plus"},
    "cadastrar/est-dotacao": {"icon": "clipboard-minus"},
    "cadastrar/planejamento/programar-pta-loa": {
        "icon": "box-arrow-up-right",
        "url": "https://pta2025.projetoswebcsg.life/",
    },
    "cadastrar/plan_21-nger/meta_fisica": {"icon": "table"},
    "cadastrar/plan_21-nger/subacao": {"icon": "diagram-3-fill"},
    "cadastrar/plan_21-nger/etapa": {"icon": "list-task"},
    "institucional": {"icon": "building"},
    "institucional/diretrizes": {"icon": "journal-text"},
    "institucional/repositorio": {"icon": "archive"},
    "institucional/legislacao": {"icon": "file-earmark-text"},
    "institucional/parceiros": {"icon": "people"},
    "relatorios": {"icon": "bar-chart-line"},
    "grupo/relatorios-planejamento": {"icon": "diagram-3"},
    "grupo/relatorios-estrutura": {"icon": "diagram-2"},
    "grupo/relatorios-execucao": {"icon": "bar-chart-steps"},
    "relatorios/plan20-seduc": {"icon": "file-earmark-spreadsheet"},
    "relatorios/plan21-nger": {"icon": "file-earmark-spreadsheet"},
    "relatorios/estrutura-planejamento": {"icon": "search"},
    "relatorios/fip613": {"icon": "file-earmark-bar-graph"},
    "relatorios/ped": {"icon": "graph-up"},
    "relatorios/emp": {"icon": "clipboard-data"},
    "relatorios/est-emp": {"icon": "clipboard-check"},
    "relatorios/nob": {"icon": "clipboard-plus"},
    "relatorios/receita-anexo10": {"icon": "file-earmark-spreadsheet"},
    "relatorios/dotacao": {"icon": "clipboard-data"},
    "relatorios/est-dotacao": {"icon": "clipboard-minus"},
    "paineis-dashboards": {"icon": "speedometer2"},
    "grupo/paineis-planejamento": {"icon": "diagram-3"},
    "paineis-dashboards/teto-orcamentario": {"icon": "bar-chart-line-fill"},
    "area-uens": {"icon": "diagram-3"},
    "area-uens/sage": {"icon": "folder2-open"},
    "usuarios": {"icon": "people"},
    "usuarios/cadastrar": {"icon": "plus-circle"},
    "usuarios/editar": {"icon": "pencil-square"},
    "usuarios/perfil": {"icon": "shield-lock"},
    "usuarios/senha": {"icon": "key"},
    "usuarios/api-acessos": {"icon": "hdd-network"},
    "painel": {"icon": "grid"},
    "logout": {"icon": "box-arrow-right"},
    "atualizar/estrutura-planejamento/programas": {"icon": "folder2-open"},
    "atualizar/estrutura-planejamento/acoes": {"icon": "list-task"},
    "atualizar/estrutura-planejamento/produtos": {"icon": "box-seam"},
    "atualizar/estrutura-planejamento/componentes": {"icon": "layers"},
    "atualizar/estrutura-planejamento/componentes-rev": {"icon": "journal-richtext"},
    "atualizar/estrutura-planejamento/vinculos": {"icon": "link-45deg"},
    "atualizar/estrutura-planejamento/modelos-chave": {"icon": "ui-checks-grid"},
    "atualizar/estrutura-planejamento/catalogo-chave": {"icon": "key"},
    "atualizar/estrutura-planejamento/replicar-exercicio": {"icon": "arrow-repeat"},
    "atualizar/teto-seduc": {"icon": "cash-stack"},
    "atualizar/plan20-seduc": {"icon": "file-earmark-spreadsheet"},
    "atualizar/chave_planejamento_regra": {"icon": "tools"},
    "atualizar/chaves_planejamento_upload": {"icon": "cloud-upload"},
    "atualizar/fip613": {"icon": "cloud-upload"},
    "atualizar/ped": {"icon": "arrow-up-circle"},
    "atualizar/emp": {"icon": "cloud-arrow-up"},
    "atualizar/receita-anexo10": {"icon": "file-spreadsheet"},
    "atualizar/est-emp": {"icon": "cloud-check"},
    "atualizar/nob": {"icon": "cloud-plus"},
    "area-uens/sage/notas-see": {"icon": "file-earmark-pdf"},
}


MENU_ROUTE_VARIANTS = {
    "cadastrar/dotacao": [
        {"route": "cadastrar/dotacao/formulario", "nome": "Formulário", "icon": "ui-checks"},
        {"route": "cadastrar/dotacao/consultar", "nome": "Consultar", "icon": "search"},
    ],
    "cadastrar/est-dotacao": [
        {"route": "cadastrar/est-dotacao/formulario", "nome": "Formulário", "icon": "ui-checks"},
        {"route": "cadastrar/est-dotacao/consultar", "nome": "Consultar", "icon": "search"},
    ],
    "cadastrar/plan_21-nger/meta_fisica": [
        {"route": "cadastrar/plan_21-nger/meta_fisica/formulario", "nome": "Formulário", "icon": "ui-checks"},
        {"route": "cadastrar/plan_21-nger/meta_fisica/consultar", "nome": "Consultar", "icon": "search"},
    ],
    "cadastrar/plan_21-nger/subacao": [
        {"route": "cadastrar/plan_21-nger/subacao/formulario", "nome": "Formulário", "icon": "ui-checks"},
        {"route": "cadastrar/plan_21-nger/subacao/consultar", "nome": "Consultar", "icon": "search"},
    ],
    "cadastrar/plan_21-nger/etapa": [
        {"route": "cadastrar/plan_21-nger/etapa/formulario", "nome": "Formulário", "icon": "ui-checks"},
        {"route": "cadastrar/plan_21-nger/etapa/consultar", "nome": "Consultar", "icon": "search"},
    ],
}


def build_menu_tree(features=None):
    """Gera o menu a partir do mesmo catálogo consumido pelo painel."""
    items = deepcopy(FEATURES if features is None else features)

    def enrich(nodes):
        for node in nodes:
            meta = MENU_META.get(node["id"], {})
            node.update(meta)
            node["dom_id"] = node["id"].replace("grupo/", "").replace("/", "-").replace("_", "-")
            node["icon"] = node.get("icon", "circle")
            node["menu_routes"] = deepcopy(MENU_ROUTE_VARIANTS.get(node["id"], []))
            enrich(node.get("children") or [])
        return nodes

    return enrich(items)
