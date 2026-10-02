const pillars=[
["Impacto Educacional","Metas PEE 5, 6, 7, 11 e 15","#e2615c"],
["Equidade e Diversidade","Metas PEE 1, 2, 3, 4, 7, 8, 9, 10 e 17","#e28c43"],
["Tecnologia e Educação","Metas PEE 2, 3, 5, 7, 9 e 17","#4f9bb5"],
["Valorização Profissional","Metas PEE 5, 15, 16, 18 e 19","#7767b5"],
["Gestão para Resultado","Metas PEE 7, 15, 19 e 20","#42a58c"],
["Infraestrutura","Metas PEE 7, 15 e 20","#267bb1"]
];
const peeGoals={
1:"Ofertar educação infantil na pré-escola para 100% das crianças de 4 e 5 anos e ampliar creches para atender no mínimo 50% das crianças de 0 a 3 anos.",
2:"Universalizar o ensino fundamental de 9 anos para a população de 6 a 14 anos e garantir que pelo menos 95% concluam essa etapa na idade recomendada.",
3:"Universalizar o atendimento escolar para a população de 15 a 17 anos e elevar a taxa líquida de matrículas no ensino médio para 85%.",
4:"Universalizar o acesso à educação básica e ao atendimento educacional especializado para a população de 4 a 17 anos com deficiência, TGD e altas habilidades ou superdotação.",
5:"Alfabetizar todas as crianças, no máximo, até o final do 3º ano do ensino fundamental.",
6:"Oferecer educação em tempo integral em no mínimo 50% das escolas públicas, atendendo pelo menos 25% dos estudantes da educação básica.",
7:"Fomentar a qualidade da educação básica nas unidades de ensino do sistema estadual, com foco na melhoria do fluxo escolar e da aprendizagem.",
8:"Elevar a escolaridade média da população de 18 a 29 anos, especialmente populações do campo, regiões de menor escolaridade e grupos historicamente vulnerabilizados.",
9:"Elevar a taxa de alfabetização da população de 15 anos ou mais e reduzir o analfabetismo absoluto e funcional.",
10:"Oferecer no mínimo 25% das matrículas de EJA nos ensinos fundamental e médio de forma integrada à educação profissional.",
11:"Ampliar as matrículas da educação profissional técnica de nível médio, assegurando qualidade da oferta e expansão no segmento público.",
12:"Elevar as taxas de matrícula na educação superior, assegurada qualidade da oferta e expansão no segmento público.",
13:"Elevar a qualidade da educação superior e ampliar a proporção de mestres e doutores no corpo docente.",
14:"Elevar gradualmente o número de matrículas em cursos de mestrado e doutorado.",
15:"Garantir formação específica inicial para que todos que atuam na educação possuam formação em nível superior.",
16:"Formar em nível de pós-graduação 50% dos professores da educação básica e garantir formação continuada aos profissionais da educação básica.",
17:"Promover continuamente o sistema único de ensino, considerando as diferentes realidades sociais e culturais dos municípios.",
18:"Garantir existência e cumprimento dos planos de carreira dos profissionais da educação pública.",
19:"Garantir a efetivação da gestão democrática na educação pública, associada a critérios técnicos de mérito e desempenho.",
20:"Garantir aplicação imediata e integral dos recursos financeiros públicos destinados à educação, com transparência pública."
};
const peeIndicators={
1:["Número de crianças de 4 a 5 anos atendidas em relação ao total de crianças nessa faixa etária.","Número de crianças de 0 a 3 anos atendidas em relação ao total de crianças nessa faixa etária.","Distribuição territorial da oferta de educação infantil."],
2:["Taxa de atendimento escolar da população de 6 a 14 anos.","Percentual de estudantes que concluem o ensino fundamental na idade recomendada.","Indicadores de fluxo escolar por ano/série."],
3:["Taxa de atendimento escolar da população de 15 a 17 anos.","Taxa líquida de matrícula no ensino médio.","Indicadores de permanência e conclusão na idade adequada."],
4:["Percentual de estudantes de 4 a 17 anos com deficiência, TGD, altas habilidades ou superdotação matriculados.","Acesso ao atendimento educacional especializado - AEE.","Oferta de recursos, serviços e condições de acessibilidade educacional."],
5:["Percentual de estudantes alfabetizados até o final do 3º ano do ensino fundamental.","Resultados de avaliações de alfabetização.","Evolução da alfabetização por escola, território e perfil de estudante."],
6:["Percentual de escolas públicas com oferta de educação em tempo integral.","Percentual de estudantes da educação básica atendidos em jornada ampliada.","Distribuição da oferta por etapa, rede e região."],
7:["Indicadores de aprendizagem e fluxo escolar.","Ideb ou indicador equivalente de qualidade educacional.","Taxas de aprovação, reprovação, abandono e desempenho em avaliações educacionais."],
8:["Escolaridade média da população de 18 a 29 anos.","Escolaridade média por população do campo, renda, raça/cor e região.","Redução das desigualdades educacionais entre grupos populacionais."],
9:["Taxa de alfabetização da população de 15 anos ou mais.","Taxa de analfabetismo absoluto.","Indicadores de analfabetismo funcional."],
10:["Percentual de matrículas da EJA integradas à educação profissional.","Oferta da EJA integrada por etapa, modalidade, município e público atendido."],
11:["Número de matrículas na educação profissional técnica de nível médio.","Percentual de expansão da oferta pública.","Distribuição territorial e qualidade da oferta dos cursos."],
12:["Taxa bruta de matrícula na educação superior.","Taxa líquida de matrícula da população de 18 a 24 anos na educação superior.","Percentual de expansão das novas matrículas no segmento público."],
13:["Percentual de docentes da educação superior com mestrado.","Percentual de docentes da educação superior com doutorado.","Composição do corpo docente por titulação."],
14:["Número de matrículas em cursos de mestrado.","Número de matrículas em cursos de doutorado.","Evolução da pós-graduação stricto sensu por área e instituição."],
15:["Percentual de profissionais da educação com formação específica inicial em nível superior.","Aderência da formação à área de atuação.","Cobertura da formação em regime de colaboração."],
16:["Percentual de professores da educação básica com pós-graduação.","Acesso dos profissionais à formação continuada.","Alinhamento da formação continuada às necessidades do sistema de ensino."],
17:["Número e abrangência das ações de cooperação federativa.","Cobertura do regime de colaboração entre Estado e municípios.","Atendimento às diferentes realidades sociais e culturais dos municípios."],
18:["Existência de planos de carreira para os profissionais da educação.","Cumprimento e atualização dos planos de carreira.","Aderência ao piso salarial nacional e às regras de valorização profissional."],
19:["Existência de normas de gestão democrática.","Processos baseados em critérios técnicos de mérito e desempenho.","Participação da comunidade escolar e transparência decisória."],
20:["Percentual de aplicação dos recursos públicos destinados à educação.","Execução orçamentária e financeira dos recursos educacionais.","Transparência, publicidade e rastreabilidade dos recursos aplicados."]
};

// eixo, código do eixo, política PAOE, código da política, pilar, código do pilar, metas PEE
const macros=[
{name:"Desenvolvimento Educacional",code:"DESENV_EDUCACIONAL",color:"#ef6b63",axes:[
["Sistema Estruturado de Ensino","E_SISTEMA_ESTRUT","Sistema Estruturado de Ensino","_SISTEMA_ESTRUT","Impacto Educacional","P_IMPACTO_","5, 6, 7, 11, 15"],
["Ensino Médio","E_ENSINO_MÉDIO","Novo Ensino Médio","_NOVO_ENSINO_MÉD","Impacto Educacional","P_IMPACTO_","5, 6, 7, 11, 15"],
["Ensino Fundamental","E_ENSINO_FUNDAMENTAL","Projetos Pedagógicos Complementares","_PROJ_PED_INTEGR","Impacto Educacional","P_IMPACTO_","5, 6, 7, 11, 15"],
["Línguas Estrangeiras","E_LÍNG_ESTRANGEIRAS","Línguas Estrangeiras","_LÍNGUAS_ESTRANG","Impacto Educacional","P_IMPACTO_","5, 6, 7, 11, 15"],
["Projetos Pedagógicos Integrados","E_PROJ_PED_INTEGRAD","Projetos Pedagógicos Complementares","_PROJ_PED_INTEGR","Tecnologia e Educação","P_TECNOLOGIA_","2, 3, 5, 7, 9, 17"]]},
{name:"Currículo Ampliado",code:"CURRÍCULO_AMPLIADO",color:"#27a99b",axes:[
["Educação em Tempo Integral","E_ESCOLA_TEMPO_INTEG","Educação em Tempo Integral","_ED_TEMPO_INTEGR","Impacto Educacional","P_IMPACTO_","5, 6, 7, 11, 15"],
["Educação Profissional e Tecnológica","E_EDUC_PROF_TEC","Novo Ensino Médio","_NOVO_ENSINO_MÉD","Impacto Educacional","P_IMPACTO_","5, 6, 7, 11, 15"],
["Tecnologia no Ambiente Escolar","E_TECNOL_AMB_ESCOLAR","Tecnologia no Ambiente Escolar","_TECNOLOGIA_ESC","Tecnologia e Educação","P_TECNOLOGIA_","2, 3, 5, 7, 9, 17"]]},
{name:"Avaliação",code:"AVALIAÇÃO",color:"#6f68b4",axes:[
["Avaliação","E_AVALIAÇÃO","Avaliação","_AVALIAÇÃO_MT","Impacto Educacional","P_IMPACTO_","5, 6, 7, 11, 15"]]},
{name:"Equidade e Diversidade",code:"EQUIDADE_DIVERSID",color:"#ed984c",axes:[
["Educação para Jovens e Adultos","E_EDUC_EJA","Educação para Jovens e Adultos","_EDUC_EJA","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"],
["Direitos Humanos","E_DIREITOS_HUMANOS","Educação para Jovens e Adultos","_EDUC_EJA","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"],
["Imigrantes","E_IMIGRANTES","Educação para Jovens e Adultos","_EDUC_EJA","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"],
["Educação Especial","E_EDUC_ESPECIAL","Educação Especial","_EDUC_ESPECIAL","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"],
["Distúrbios de Aprendizagem","E_DISTÚRB_APRENDIZ","Educação Especial","_EDUC_ESPECIAL","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"],
["Superdotação e Altas Habilidades","E_ALTAS_HABILIDADES","Educação Especial","_EDUC_ESPECIAL","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"],
["Educação Indígena","E_EDUC_INDÍGENA","Educação Indígena","_EDUC_INDÍGENA","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"],
["Educação Quilombola","E_EDUC_QUILOMBOLA","Educação Quilombola","_EDUC_QUILOMBOLA","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"],
["Educação do Campo","E_EDUC_CAMPO","Educação do Campo","_EDUC_CAMPO","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"]]},
{name:"Acesso e Permanência",code:"ACESSO_E_PERM",color:"#e96562",axes:[
["Busca Ativa","E_BUSCA_ATIVA","Acesso e Permanência","_ACESSO_E_PERM","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"],
["Materiais e Uniformes Escolares","E_MATERIAIS_UNIFORM","Uniformes Escolares","_UNIFORMES_","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"],
["Materiais e Uniformes Escolares","E_MATERIAIS_UNIFORM","Materiais Escolares","_MATERIAIS_","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"],
["Alimentação Escolar","E_ALIMENTAÇÃO_","Alimentação Escolar","_ALIMENTAÇÃO_","Infraestrutura","P_INFRAESTR_","7, 15, 20"]]},
{name:"Promoção da Cultura de Paz",code:"CULTURA_DE_PAZ",color:"#21a69c",axes:[
["Promoção da Cultura de Paz","E_CULTURA_DE_PAZ","Bem-Estar Social","_BEM-ESTAR_","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"],
["Bem-Estar Escolar","E_BEM-ESTAR_ESCOLAR","Bem-Estar Social","_BEM-ESTAR_","Equidade e Diversidade","P_EQUIDADE_","1, 2, 3, 4, 7, 8, 9, 10, 17"]]},
{name:"Gestão e Inovação",code:"GESTÃO_INOVAÇÃO",color:"#625ca7",axes:[
["Escolas Militares e Cívico-Militares","E_ESCOLAS_MILITARES","Escolas Militares","_ESCOLAS_MILITAR","Impacto Educacional","P_IMPACTO_","5, 6, 7, 11, 15"],
["Gestão Escolar","E_GESTÃO_ESCOLAR","Gestão Escolar","_GESTÃO_ESCOLAR","Gestão para Resultado","P_GESTÃO_","7, 15, 19, 20"],
["Gestão Integrada","E_GESTÃO_INTEGRADA","Gestão Integrada","_GESTÃO_INTEGR","Gestão para Resultado","P_GESTÃO_","7, 15, 19, 20"]]},
{name:"Regime de Colaboração",code:"REGIME_COLABORAÇÃO",color:"#dd7e3e",axes:[
["Alfabetização","E_ALFABETIZAÇÃO","Alfabetização","_ALFABETIZAÇÃO","Impacto Educacional","P_IMPACTO_","5, 6, 7, 11, 15"],
["Regime de Colaboração","E_REGIME_COLABORAÇÃO","Regime de Colaboração","_REGIME_COLAB","Gestão para Resultado","P_GESTÃO_","7, 15, 19, 20"],
["Transporte Escolar","E_TRANSPORTE_ESCOLAR","Transporte Escolar","_TRANSPORTE_","Infraestrutura","P_INFRAESTR_","7, 15, 20"]]},
{name:"Valorização Profissional",code:"VALORIZAÇÃO_PROF",color:"#7364b2",axes:[
["Formação Continuada de Professores","E_FORMAÇÃO_DE_PROF","Formação Continuada de Professores","_FORMAÇÃO_PROF","Valorização Profissional","P_VALORIZ_PRO","5, 15, 16, 18, 19"],
["Valorização Profissional","E_VALORIZAÇÃO_PROF","Valorização Profissional","_VALORIZ_PROF","Valorização Profissional","P_VALORIZ_PRO","5, 15, 16, 18, 19"],
["Gestão Estratégica de Pessoas","E_GESTÃO_DE_PESSOAS","Descentralização da Gestão de Pessoas","_GESTÃO_PESSOAS","Valorização Profissional","P_VALORIZ_PRO","5, 15, 16, 18, 19"]]},
{name:"Infraestrutura Escolar",code:"INFRAESTRUTURA",color:"#168f85",axes:[
["Infraestrutura Escolar","E_INFRAESTUTURA_ESC","Infraestrutura Escolar","_INFRAESTRUTURA","Infraestrutura","P_INFRAESTR_","7, 15, 20"],
["Gestão do Patrimônio","E_GESTÃO_DO_PATRIM","Gestão do Patrimônio","_GESTÃO_PATRIM","Infraestrutura","P_INFRAESTR_","7, 15, 20"]]}
];

const steps=[["Direcionamento","Macropolítica e política"],["Regionalização","Regiões e metas físicas"],["Subações","Entregas anuais"],["Etapas","Marcos de execução"],["Orçamento","Recursos necessários"],["Revisão","Validar e concluir"]];
const regions=[["R100","Região 100"],["R200","Região 200"],["R300","Região 300"],["R400","Região 400"],["R500","Região 500"],["R600","Região 600"],["R700","Região 700"],["R800","Região 800"],["R900","Região 900"],["R1000","Região 1000"],["R1100","Região 1100"],["R1200","Região 1200"],["R9900","Todo o Estado"]];
const state={macro:null,axis:null,step:0,unit:"2",adjunct:"SAIP",subfunction:"361",regions:[],target:"",subactions:[],stages:[],budget:"",contextGoalsOpen:false,contextPolicyOpen:false,contextOpenGoals:[]};
const pta2027DraftKey="spo-pta-2027-draft";
const currentUser={name:"Jean Carlos Alves Figueiredo",role:"admin"};
const defaultStrategicElements={
 mission:"Garantir acesso, permanência e aprendizagem com qualidade e equidade.",
 vision:"Estar entre as cinco melhores redes públicas, com evidências e o estudante no centro.",
 business:"Aprendizagem e formação de cidadãos proativos para uma sociedade mais justa.",
 values:"Democracia · Resultado · Diversidade · Equidade · Ética · Inovação",
 riskIntro:"A Política de Gestão de Riscos reforça que objetivos, processos, projetos, produtos e controles precisam caminhar juntos.",
 riskObjectives:"Riscos devem ser avaliados pelo impacto sobre missão, continuidade e resultados institucionais.",
 riskProcesses:"A implementação considera macroprocessos, processos, projetos e produtos gerenciados pela SEDUC.",
 riskControls:"As respostas aos riscos devem equilibrar custo-benefício, prevenção, correção e contingência.",
 riskMonitoring:"Indicadores, desvios e análise crítica alimentam a melhoria contínua da gestão."
};
const supportTextFields=[
 {group:"Mapa EAP",key:"eap.map.eyebrow",label:"Marcador do Mapa EAP",type:"input",value:"MAPA EAP"},
 {group:"Mapa EAP",key:"eap.map.title",label:"Título do Mapa EAP",value:"Estrutura analítica da programação estratégica"},
 {group:"Mapa EAP",key:"eap.map.description",label:"Descrição do Mapa EAP",value:"A EAP organiza a passagem entre a estratégia, os vínculos de planejamento e a programação operacional que alimenta o PTA."},
 {group:"Mapa EAP",key:"eap.flow.1.title",label:"Fluxo 1 - Título",type:"input",value:"Classificação estratégica"},
 {group:"Mapa EAP",key:"eap.flow.1.summary",label:"Fluxo 1 - Resumo",value:"Suporte, gerencial ou finalística."},
 {group:"Mapa EAP",key:"eap.flow.1.detail",label:"Fluxo 1 - Detalhe",value:"Define a função estratégica do agrupamento no PTA e evita confundir macropolítica com processo organizacional."},
 {group:"Mapa EAP",key:"eap.flow.2.title",label:"Fluxo 2 - Título",type:"input",value:"Macropolítica"},
 {group:"Mapa EAP",key:"eap.flow.2.summary",label:"Fluxo 2 - Resumo",value:"Agrupa políticas na visão estratégica atual."},
 {group:"Mapa EAP",key:"eap.flow.2.detail",label:"Fluxo 2 - Detalhe",value:"É a camada de comunicação e gestão que organiza as políticas educacionais em grandes frentes."},
 {group:"Mapa EAP",key:"eap.flow.3.title",label:"Fluxo 3 - Título",type:"input",value:"Política PAOE"},
 {group:"Mapa EAP",key:"eap.flow.3.summary",label:"Fluxo 3 - Resumo",value:"Conecta a estratégia ao produto programável."},
 {group:"Mapa EAP",key:"eap.flow.3.detail",label:"Fluxo 3 - Detalhe",value:"É o ponto de ligação entre a política educacional e o produto/ação que será programado."},
 {group:"Mapa EAP",key:"eap.flow.4.title",label:"Fluxo 4 - Título",type:"input",value:"Componente"},
 {group:"Mapa EAP",key:"eap.flow.4.summary",label:"Fluxo 4 - Resumo",value:"Detalha o conteúdo programável da entrega."},
 {group:"Mapa EAP",key:"eap.flow.4.detail",label:"Fluxo 4 - Detalhe",value:"É a menor unidade estratégica da EAP que pode ser vinculada explicitamente a subações/entregas."},
 {group:"Mapa EAP",key:"eap.flow.5.title",label:"Fluxo 5 - Título",type:"input",value:"Programação PTA"},
 {group:"Mapa EAP",key:"eap.flow.5.summary",label:"Fluxo 5 - Resumo",value:"Regiões, metas físicas, subações, etapas e orçamento."},
 {group:"Mapa EAP",key:"eap.flow.5.detail",label:"Fluxo 5 - Detalhe",value:"Transforma o componente em entrega concreta, com responsáveis, prazos e recursos."},
 {group:"Mapa EAP",key:"eap.flow.6.title",label:"Fluxo 6 - Título",type:"input",value:"Chave de planejamento"},
 {group:"Mapa EAP",key:"eap.flow.6.summary",label:"Fluxo 6 - Resumo",value:"Rastreabilidade estratégica na execução."},
 {group:"Mapa EAP",key:"eap.flow.6.detail",label:"Fluxo 6 - Detalhe",value:"Codifica a decisão de planejamento para permitir leitura estratégica da execução física e financeira."},
 {group:"Mapa EAP",key:"eap.layer.orientation.title",label:"Camada de orientação - Título",type:"input",value:"Camada de orientação"},
 {group:"Mapa EAP",key:"eap.layer.orientation.text",label:"Camada de orientação - Texto",value:"Pilares estratégicos, metas PEE e indicadores ajudam a qualificar o sentido da programação."},
 {group:"Mapa EAP",key:"eap.layer.governance.title",label:"Camada de governança - Título",type:"input",value:"Camada de governança"},
 {group:"Mapa EAP",key:"eap.layer.governance.text",label:"Camada de governança - Texto",value:"Alterações na EAP podem ser propostas por administrador e ficam pendentes para validação por outro administrador."},
 {group:"Mapa EAP",key:"eap.layer.integration.title",label:"Camada de integração - Título",type:"input",value:"Camada de integração"},
 {group:"Mapa EAP",key:"eap.layer.integration.text",label:"Camada de integração - Texto",value:"A mesma estrutura deve alimentar a cadeia de valor, a tela de programação e os dados normalizados do SPO real."},
 {group:"Visão estratégica",key:"strategy.view.eyebrow",label:"Marcador da visão estratégica",type:"input",value:"VISÃO ESTRATÉGICA"},
 {group:"Visão estratégica",key:"strategy.view.title",label:"Título da visão estratégica",value:"A programação nasce da estratégia"},
 {group:"Visão estratégica",key:"strategy.view.description",label:"Descrição da visão estratégica",value:"A cadeia estratégica fica no topo da experiência. A cadeia organizacional aparece como camada que viabiliza a execução."},
 {group:"Visão estratégica",key:"strategy.view.diagramTitle",label:"Título do diagrama estratégico",type:"input",value:"Diagrama da cadeia estratégica de programação"},
 {group:"Visão processual",key:"process.view.eyebrow",label:"Marcador da visão processual",type:"input",value:"VISÃO PROCESSUAL"},
 {group:"Visão processual",key:"process.view.title",label:"Título da visão processual",value:"Os processos organizacionais sustentam a execução"},
 {group:"Visão processual",key:"process.view.description",label:"Descrição da visão processual",value:"Na fase das etapas, o usuário pode associar cada entrega aos processos, responsáveis, riscos, controles e evidências necessários."},
 {group:"Visão processual",key:"process.support.summary",label:"Resumo - Processos de suporte",value:"Aquisições, contratos, finanças, patrimônio, pessoas, documentos, transporte e serviços."},
 {group:"Visão processual",key:"process.managerial.summary",label:"Resumo - Processos gerenciais",value:"Gestão estratégica, monitoramento, controle interno, ouvidoria, correição, comunicação e articulação."},
 {group:"Visão processual",key:"process.finalistic.summary",label:"Resumo - Processos finalísticos",value:"Atividades ligadas à entrega educacional, atendimento, aprendizagem e valor público."},
 {group:"Visão processual",key:"process.support.items",label:"Lista - Processos de suporte",value:"Aquisições e contratos\nInfraestrutura e patrimônio\nContabilidade\nGestão de documentos\nGestão de pessoas\nConvênios\nFinanças\nServiços\nTransporte administrativo\nOrçamento"},
 {group:"Visão processual",key:"process.support.value",label:"Proposta de valor - Processos de suporte",value:"Assegurar a execução de serviços administrativos de excelência, apoiando a área finalística no alcance dos seus resultados e na qualidade dos serviços públicos prestados no âmbito da Secretaria de Estado de Educação."},
 {group:"Visão processual",key:"process.support.clients",label:"Clientes - Processos de suporte",value:"Comunidade escolar e servidores públicos da Educação."},
 {group:"Visão processual",key:"process.managerial.items",label:"Lista - Processos gerenciais",value:"Articulação institucional\nCerimonial\nÉtica\nDesenvolvimento organizacional\nApoio estratégico\nGestão estratégica para resultados\nControle interno\nCorreição\nCoordenação do programa\nOuvidoria\nComunicação\nJurídico"},
 {group:"Visão processual",key:"process.managerial.value",label:"Proposta de valor - Processos gerenciais",value:"Promover a gestão e a governança institucional na Secretaria de Educação."},
 {group:"Visão processual",key:"process.managerial.clients",label:"Clientes - Processos gerenciais",value:"Alunos, pais ou responsáveis, servidores públicos da Educação, instituições do sistema público e privado de ensino, Ministério da Educação, Prefeituras, Conselhos Nacionais e Estaduais de Educação."},
 {group:"Visão processual",key:"process.finalistic.items",label:"Lista - Processos finalísticos",value:"Desenvolvimento educacional\nCurrículo ampliado\nAvaliação educacional\nEquidade e diversidade\nPromoção da cultura de paz\nGestão e inovação\nRegime de colaboração\nAcesso e permanência"},
 {group:"Visão processual",key:"process.finalistic.value",label:"Proposta de valor - Processos finalísticos",value:"Propor e executar políticas públicas voltadas à educação básica, assegurando ao estudante o desenvolvimento de capacidades e a construção de conhecimentos para a formação de valores humanos na conquista da cidadania."},
 {group:"Visão processual",key:"process.finalistic.clients",label:"Clientes - Processos finalísticos",value:"Alunos, pais ou responsáveis, servidores públicos da Educação, instituições do sistema público e privado de ensino, Ministério da Educação, Prefeituras, Conselhos Nacionais e Estaduais de Educação."},
 {group:"Visão processual",key:"process.chain.label",label:"Encadeamento ilustrativo - Título",type:"input",value:"Encadeamento ilustrativo"},
 {group:"Visão processual",key:"process.chain.text",label:"Encadeamento ilustrativo - Texto",value:"Processo → proposta de valor → cliente → etapa programada → evidência de execução"},
 {group:"Visão processual",key:"process.diagram.title",label:"Título do diagrama processual",type:"input",value:"Diagrama de integração entre cadeias"},
 {group:"Visão processual",key:"process.examples.title",label:"Título dos exemplos",type:"input",value:"Exemplos de ligação nas etapas"},
 {group:"Visão processual",key:"process.examples.items",label:"Exemplos de ligação",value:"Instruir contratação → Aquisições e contratos\nAcompanhar obra → Infraestrutura e patrimônio\nMonitorar risco → Controle interno / gestão de riscos\nFormalizar parceria → Convênios / articulação institucional"}
];
const defaultSupportTexts=Object.fromEntries(supportTextFields.map(f=>[f.key,f.value]));
let eapRows=[];
let dynamicMacros=[];
let adminEditIndex=-1;
let adminConfirmToken=null;
const $=s=>document.querySelector(s);
const money=v=>Number(v||0).toLocaleString("pt-BR",{style:"currency",currency:"BRL"});

function loadStrategicElements(){
 const saved=localStorage.getItem("spo-strategic-elements");
 if(saved){try{return {...defaultStrategicElements,...JSON.parse(saved)}}catch(e){}}
 return {...defaultStrategicElements};
}
function saveStrategicElements(data){
 localStorage.setItem("spo-strategic-elements",JSON.stringify(data));
}
function loadPendingStrategicChange(){
 const saved=localStorage.getItem("spo-strategic-pending");
 if(saved){try{return JSON.parse(saved)}catch(e){}}
 return null;
}
function savePendingStrategicChange(data){
 if(data)localStorage.setItem("spo-strategic-pending",JSON.stringify(data));
 else localStorage.removeItem("spo-strategic-pending");
}
function loadSupportTexts(){
 const saved=localStorage.getItem("spo-support-texts");
 if(saved){try{return {...defaultSupportTexts,...JSON.parse(saved)}}catch(e){}}
 return {...defaultSupportTexts};
}
function saveSupportTexts(data){
 localStorage.setItem("spo-support-texts",JSON.stringify(data));
}
function loadPendingSupportTextChange(){
 const saved=localStorage.getItem("spo-support-texts-pending");
 if(saved){try{return JSON.parse(saved)}catch(e){}}
 return null;
}
function savePendingSupportTextChange(data){
 if(data)localStorage.setItem("spo-support-texts-pending",JSON.stringify(data));
 else localStorage.removeItem("spo-support-texts-pending");
}
function textParam(key){
 return loadSupportTexts()[key]??defaultSupportTexts[key]??"";
}
function textListParam(key){
 return String(textParam(key)||"").split(/\r?\n/).map(v=>v.trim()).filter(Boolean);
}
function renderStrategicElements(){
 const data=loadStrategicElements();
 const pairs=[
  ["identity-mission","mission"],["identity-vision","vision"],["identity-business","business"],["identity-values","values"],
  ["risk-intro","riskIntro"],["risk-objectives","riskObjectives"],["risk-processes","riskProcesses"],["risk-controls","riskControls"],["risk-monitoring","riskMonitoring"]
 ];
 pairs.forEach(([id,key])=>{const el=$("#"+id);if(el)el.textContent=data[key]||""});
 return data;
}
function populateStrategicEditor(){
 const data=loadStrategicElements();
 fillStrategicEditor(data);
}
function fillStrategicEditor(data){
 const map={
  "strategy-mission":"mission","strategy-vision":"vision","strategy-business":"business","strategy-values":"values",
  "strategy-risk-intro":"riskIntro","strategy-risk-objectives":"riskObjectives","strategy-risk-processes":"riskProcesses","strategy-risk-controls":"riskControls","strategy-risk-monitoring":"riskMonitoring"
 };
 Object.entries(map).forEach(([id,key])=>{const el=$("#"+id);if(el)el.value=data[key]||""});
}
function readStrategicEditor(){
 return {
  mission:readAdminValue("strategy-mission"),
  vision:readAdminValue("strategy-vision"),
  business:readAdminValue("strategy-business"),
  values:readAdminValue("strategy-values"),
  riskIntro:readAdminValue("strategy-risk-intro"),
  riskObjectives:readAdminValue("strategy-risk-objectives"),
  riskProcesses:readAdminValue("strategy-risk-processes"),
  riskControls:readAdminValue("strategy-risk-controls"),
  riskMonitoring:readAdminValue("strategy-risk-monitoring")
 };
}
function populateSupportTextEditor(){
 renderSupportTextEditor(loadSupportTexts());
}
function renderSupportTextEditor(data){
 const panel=$("#support-text-editor");
 if(!panel)return;
 const groups=[...new Set(supportTextFields.map(f=>f.group))];
 panel.innerHTML=groups.map(group=>{
  const fields=supportTextFields.filter(f=>f.group===group);
  return `<section class="support-text-group"><h5>${group}</h5><div class="support-text-grid">${fields.map(field=>{
   const value=data[field.key]??field.value??"";
   const inputId=`support-text-${field.key.replace(/[^a-z0-9]+/gi,"-")}`;
   const keyTag=`<small>${field.key}</small>`;
   if(field.type==="input")return `<label><span>${field.label}</span>${keyTag}<input data-support-key="${field.key}" id="${inputId}" value="${htmlSafe(value)}"></label>`;
   return `<label class="wide"><span>${field.label}</span>${keyTag}<textarea data-support-key="${field.key}">${htmlSafe(value)}</textarea></label>`;
  }).join("")}</div></section>`;
 }).join("");
}
function fillSupportTextEditor(data){
 renderSupportTextEditor({...defaultSupportTexts,...data});
 $("#support-text-justification").value="";
}
function readSupportTextEditor(){
 const data={};
 document.querySelectorAll("[data-support-key]").forEach(field=>{data[field.dataset.supportKey]=field.value.trim()});
 return data;
}
function renderPillars(){
 $("#pillar-list").innerHTML=pillars.map((p,i)=>`<button class="pillar" data-pillar="${i}" style="--pillar-color:${p[2]}" type="button"><strong>${p[0]}</strong><span>${p[1]}</span><em>Ver metas PEE →</em></button>`).join("");
 document.querySelectorAll(".pillar").forEach(b=>b.onclick=()=>expandPillarGoals(+b.dataset.pillar));
}
function renderPeeIndicators(goal){
 const items=peeIndicators[goal]||["Indicadores ainda não cadastrados para esta meta."];
 return `<div class="indicator-lines">${items.map((text,index)=>`<p><strong>INDICADOR ${goal}.${String.fromCharCode(65+index)}</strong><span>${text}</span></p>`).join("")}</div>`;
}
function expandPillarGoals(index){
 const current=$("#pillar-goals").dataset.pillar;
 if(current===String(index)){
  $("#pillar-goals").classList.add("hidden");
  $("#pillar-goals").dataset.pillar="";
  document.querySelectorAll(".pillar").forEach(p=>p.classList.remove("active"));
  return;
 }
 const pillar=pillars[index];
 document.querySelectorAll(".pillar").forEach((p,i)=>p.classList.toggle("active",i===index));
 const nums=(pillar[1].match(/\d+/g)||[]).map(Number);
 const panel=$("#pillar-goals");
 panel.classList.remove("hidden");
 panel.dataset.pillar=String(index);
 panel.style.setProperty("--active-pillar-color",pillar[2]);
 panel.innerHTML=`<div class="pillar-goals-head"><div><span class="eyebrow">METAS DO PLANO ESTADUAL DE EDUCAÇÃO</span><h3>${pillar[0]}</h3><p>${pillar[1]} · Lei nº 11.422/2021</p></div></div><div class="goal-list">${nums.map(n=>`<article class="goal-stack"><button class="goal-card linked-card" data-goal="${n}" type="button"><span>Meta ${n}</span><p>${peeGoals[n]||"Texto da meta não localizado."}</p><em>Ver indicadores ↓</em></button><div class="indicator-card hidden" data-indicators="${n}"><span>Indicadores da meta ${n}</span>${renderPeeIndicators(n)}</div></article>`).join("")}</div>`;
 panel.querySelectorAll(".goal-card").forEach(btn=>btn.onclick=()=>toggleGoalIndicators(btn,panel));
 panel.scrollIntoView({behavior:"smooth",block:"nearest"});
}
function toggleGoalIndicators(btn,scope=document){
 const goal=btn.dataset.goal;
 const card=scope.querySelector(`[data-indicators="${goal}"]`);
 const open=card&&!card.classList.contains("hidden");
 btn.classList.toggle("open",!open);
 if(card)card.classList.toggle("hidden",open);
 const label=btn.querySelector("em");
 if(label)label.textContent=open?"Ver indicadores ↓":"Ocultar indicadores ↑";
}
function renderMacros(){
 dynamicMacros=buildMacrosFromEap();
 const groups=[
  {title:"A. Macropolíticas de suporte",desc:"Organizam permanência, ambiente escolar, pessoas, infraestrutura e condições para viabilizar as entregas.",cls:"support",color:"#49b89c",process:"A. Suporte"},
  {title:"B. Macropolíticas gerenciais",desc:"Orientam, articulam, monitoram e governam a execução da estratégia.",cls:"managerial",color:"#0f7c68",process:"B. Gerencial"},
  {title:"C. Macropolíticas finalísticas",desc:"Concentram as entregas educacionais diretamente percebidas por estudantes, escolas e sociedade.",cls:"finalistic",color:"#1f9b7e",process:"C. Finalístico"}
 ];
 $("#macro-grid").innerHTML=`<div class="value-chain">${groups.map(g=>{const items=dynamicMacros.map((m,i)=>({m,i})).filter(x=>x.m.process===g.process);return `<section class="process-lane ${g.cls}" style="--lane-color:${g.color}"><h3>${g.title}</h3><p>${g.desc}</p><div class="macro-grid-inner">${items.length?items.map(x=>macroCard(x.m,x.i)).join(""):`<div class="empty-process">Sem macropolíticas vinculadas.</div>`}</div></section>`}).join("")}</div>`;
 document.querySelectorAll(".macro-card").forEach(b=>b.onclick=()=>expandMacro(+b.dataset.macro));
}
function buildMacrosFromEap(){
 const base=eapRows.length?eapRows:(window.EAP_ROWS||[]);
 const source=base.filter(isActiveEapRow);
 const byMacro=new Map();
 source.forEach(r=>{
  if(!byMacro.has(r.macro))byMacro.set(r.macro,{name:r.macro,code:r.macroCode||slugCode(r.macro),color:macroColor(r.process),process:r.process,axes:[]});
  const macro=byMacro.get(r.macro);
  const axisKey=[r.axis,r.policy,r.pillar,r.pee].join("|");
  if(!macro.axes.some(a=>a.key===axisKey))macro.axes.push({key:axisKey,data:[r.axis,r.axisCode||slugCode(r.axis),r.policy,r.policyCode||slugCode(r.policy),cleanPillar(r.pillar),r.pillarCode||slugCode(r.pillar),r.pee]});
 });
 return [...byMacro.values()].sort((a,b)=>processOrder(a.process)-processOrder(b.process)||a.name.localeCompare(b.name,"pt-BR")).map(m=>({...m,axes:m.axes.map(a=>a.data)}));
}
function processOrder(p){return {"A. Suporte":1,"B. Gerencial":2,"C. Finalístico":3}[p]||9}
function macroColor(process){return process==="A. Suporte"?"#49b89c":process==="B. Gerencial"?"#0f7c68":"#1f9b7e"}
function cleanPillar(p){return String(p||"").replace(/^\d+\s*-\s*/,"")||"Pilar não informado"}
function slugCode(text){return String(text||"").normalize("NFD").replace(/[\u0300-\u036f]/g,"").toUpperCase().replace(/[^A-Z0-9]+/g,"_").replace(/^_|_$/g,"").slice(0,22)}
function isPendingDelete(row){return row?._status==="pending-delete"||row?._policyStatus==="pending-delete"||row?._macroStatus==="pending-delete"}
function isActiveEapRow(row){return row&&row.process!=="Histórico"&&!isPendingDelete(row)&&row.macro&&!String(row.macro).startsWith("(")}
function activeRowsForPolicy(macro,policy){return eapRows.filter(r=>isActiveEapRow(r)&&r.macro===macro&&r.policy===policy)}
function activeRowsForMacro(macro){return eapRows.filter(r=>isActiveEapRow(r)&&r.macro===macro)}
function currentRowsForPolicy(macro,policy){return eapRows.filter(r=>r.process!=="Histórico"&&r.macro===macro&&r.policy===policy)}
function currentRowsForMacro(macro){return eapRows.filter(r=>r.process!=="Histórico"&&r.macro===macro)}
function setupEapTable(){
 if(!window.EAP_ROWS)return;
 eapRows=loadEapRows();
 renderMacros();
 document.body.classList.toggle("is-admin",currentUser.role==="admin");
 updateEapMacroOptions();
 updateEapPolicyOptions();
 $("#chain-tab").onclick=()=>showStructureView("chain");
 $("#eap-tab").onclick=()=>showStructureView("eap");
 $("#chain-map").onclick=()=>showChainSubView("map");
 $("#concept-strategy").onclick=()=>renderConceptPanel("strategy");
 $("#concept-process").onclick=()=>renderConceptPanel("process");
 $("#eap-map-tab").onclick=()=>showEapSubView("map");
 $("#eap-table-tab").onclick=()=>showEapSubView("table");
 $("#eap-search").oninput=renderEapTable;
 $("#eap-process").onchange=()=>{updateEapMacroOptions();updateEapPolicyOptions();renderEapTable()};
 $("#eap-macro").onchange=()=>{syncEapProcessFromMacro();updateEapMacroOptions();updateEapPolicyOptions();renderEapTable()};
 $("#eap-policy").onchange=()=>{syncEapFromPolicy();updateEapMacroOptions();updateEapPolicyOptions();renderEapTable()};
 $("#eap-admin-edit").onclick=()=>{const panel=$("#eap-admin-panel");const open=panel.classList.toggle("hidden")===false;$("#eap-panel").classList.toggle("admin-open",open);if(open){populateAdminEditor();panel.scrollTop=0;panel.scrollIntoView({block:"start",behavior:"smooth"})}};
 $("#eap-admin-cancel").onclick=closeAdminPanel;
 $("#eap-admin-save").onclick=saveEapAdminChange;
 $("#admin-operation").onchange=()=>{updateAdminOperationMode();refreshAdminSourceOptions()};
 $("#admin-source-process").onchange=()=>{adminConfirmToken=null;populateAdminMacros();renderAdminScopeHint()};
 $("#admin-source-macro").onchange=()=>{adminConfirmToken=null;populateAdminPolicies();renderAdminScopeHint()};
 $("#admin-source-policy").onchange=()=>{adminConfirmToken=null;populateAdminRows();renderAdminScopeHint()};
 $("#admin-source-row").onchange=()=>loadSelectedAdminRow();
 $("#admin-add-component").onclick=addAdminComponentLine;
 $("#admin-deactivation-scope").onchange=()=>{adminConfirmToken=null;refreshAdminSourceOptions();renderAdminScopeHint()};
 $("#strategy-save").onclick=()=>requestStrategicChange(readStrategicEditor(),"alteração dos elementos estratégicos");
 $("#strategy-reset").onclick=()=>{fillStrategicEditor(defaultStrategicElements);adminConfirmToken=null;toast("Campos restaurados no formulário. Para efetivar, clique em salvar e envie para revisão.")};
 $("#support-text-save").onclick=()=>requestSupportTextChange(readSupportTextEditor(),"alteração dos textos de apoio e visão gerencial");
 $("#support-text-reset").onclick=()=>{fillSupportTextEditor(defaultSupportTexts);adminConfirmToken=null;toast("Textos restaurados no formulário. Para efetivar, clique em salvar e envie para revisão.")};
 renderEapTable();
 renderPendingReview();
}
function loadEapRows(){
 const saved=localStorage.getItem("spo-eap-rows");
 if(saved){try{return JSON.parse(saved)}catch(e){}}
 return [...(window.EAP_ROWS||[])];
}
function persistEapRows(){
 localStorage.setItem("spo-eap-rows",JSON.stringify(eapRows));
}
function closeAdminPanel(){
 $("#eap-admin-panel")?.classList.add("hidden");
 $("#eap-panel")?.classList.remove("admin-open");
}
function optionList(values,emptyLabel){
 return `<option value="">${emptyLabel}</option>`+[...new Set(values.filter(Boolean))].map(v=>`<option>${v}</option>`).join("");
}
function adminDeletionScope(){return $("#admin-deactivation-scope")?.value||"component"}
function adminSourceRows(){
 const op=$("#admin-operation")?.value||"include";
 const scope=adminDeletionScope();
 if(op!=="disable")return eapRows;
 return eapRows.filter(r=>{
  if(r.process==="Histórico"||r._macroStatus==="pending-delete")return false;
  if(scope==="macro")return true;
  if(scope==="policy")return r._policyStatus!=="pending-delete";
  return !isPendingDelete(r);
 });
}
function populateAdminEditor(){
 updateAdminOperationMode();
 populateStrategicEditor();
 populateSupportTextEditor();
 refreshAdminSourceOptions();
}
function refreshAdminSourceOptions(){
 if(["strategy","supportTexts"].includes($("#admin-operation")?.value||"include"))return;
 const source=adminSourceRows();
 $("#admin-source-process").innerHTML=optionList(source.map(r=>r.process),"Selecione");
 $("#admin-source-process").value=$("#eap-process").value||"";
 populateAdminMacros();
}
function updateAdminOperationMode(){
 adminConfirmToken=null;
 const op=$("#admin-operation")?.value||"include";
 const selector=document.querySelector(".admin-selector");
 const destructive=op==="disable";
 const strategy=op==="strategy";
 const supportTexts=op==="supportTexts";
 const configMode=strategy||supportTexts;
 if(selector)selector.classList.toggle("muted-section",op==="include");
 selector?.classList.toggle("hidden",configMode);
 $(".admin-compare")?.classList.toggle("hidden",configMode);
 $("#admin-justification-wrap")?.classList.toggle("hidden",configMode);
 $(".eap-admin-actions")?.classList.toggle("hidden",configMode);
 $(".admin-safety-note")?.classList.toggle("hidden",configMode);
 $(".strategy-admin")?.classList.toggle("hidden",!strategy);
 $(".support-text-admin")?.classList.toggle("hidden",!supportTexts);
 $("#admin-deactivation-scope-wrap")?.classList.toggle("hidden",!destructive||configMode);
 setAdminProposalDisabled(destructive);
 if(op==="include"){adminEditIndex=-1;renderAdminCurrent(null);clearAdminProposal()}
 if(destructive)clearAdminProposal();
 const save=$("#eap-admin-save");
 if(save)save.textContent=op==="disable"?"Solicitar desativação":"Confirmar inclusão local";
 const strategySave=$("#strategy-save");
 if(strategySave)strategySave.textContent="Salvar elementos estratégicos";
 if(strategy)populateStrategicEditor();
 else if(supportTexts)populateSupportTextEditor();
 else renderAdminScopeHint();
}
function setAdminProposalDisabled(disabled){
 const proposalIds=["admin-process","admin-macro","admin-macro-code","admin-axis","admin-axis-code","admin-policy","admin-policy-code","admin-pillar","admin-pillar-code","admin-pee","admin-add-component"];
 proposalIds.forEach(id=>{const el=$("#"+id);if(el)el.disabled=disabled});
 document.querySelectorAll(".admin-component-input").forEach(input=>input.disabled=disabled);
 const proposal=document.querySelector(".admin-compare>div:nth-child(2)");
 if(proposal)proposal.classList.toggle("proposal-disabled",disabled);
}
function populateAdminMacros(){
 const process=$("#admin-source-process").value;
 const rows=adminSourceRows().filter(r=>!process||r.process===process);
 $("#admin-source-macro").innerHTML=optionList(rows.map(r=>r.macro),"Selecione");
 if($("#eap-macro").value&&rows.some(r=>r.macro===$("#eap-macro").value))$("#admin-source-macro").value=$("#eap-macro").value;
 populateAdminPolicies();
}
function populateAdminPolicies(){
 const process=$("#admin-source-process").value;
 const macro=$("#admin-source-macro").value;
 const rows=adminSourceRows().filter(r=>(!process||r.process===process)&&(!macro||r.macro===macro));
 $("#admin-source-policy").innerHTML=optionList(rows.map(r=>r.policy),"Selecione");
 if($("#eap-policy").value&&rows.some(r=>r.policy===$("#eap-policy").value))$("#admin-source-policy").value=$("#eap-policy").value;
 populateAdminRows();
}
function populateAdminRows(){
 const process=$("#admin-source-process").value;
 const macro=$("#admin-source-macro").value;
 const policy=$("#admin-source-policy").value;
 const source=adminSourceRows();
 const rows=eapRows.map((r,i)=>({...r,__index:i})).filter(r=>source.includes(eapRows[r.__index])&&(!process||r.process===process)&&(!macro||r.macro===macro)&&(!policy||r.policy===policy));
 $("#admin-source-row").innerHTML=`<option value="">Selecione</option>`+rows.map(r=>`<option value="${r.__index}">${r.axis} · ${r.policy} · ${r.comp2024}</option>`).join("");
 if(rows.length){$("#admin-source-row").value=rows[0].__index;loadSelectedAdminRow()} else {adminEditIndex=-1;renderAdminCurrent(null)}
}
function loadSelectedAdminRow(){
 adminConfirmToken=null;
 const idx=Number($("#admin-source-row").value);
 adminEditIndex=Number.isFinite(idx)?idx:-1;
 const row=eapRows[adminEditIndex];
 renderAdminCurrent(row);
 clearAdminProposal();
 renderAdminScopeHint();
}
function renderAdminCurrent(row){
 const fields=[["Classificação estratégica","process"],["Macropolítica","macro"],["Código macro","macroCode"],["Eixo","axis"],["Código eixo","axisCode"],["Política","policy"],["Código política","policyCode"],["Pilar","pillar"],["Código pilar","pillarCode"],["Metas PEE","pee"],["Componente 2024","comp2024"]];
 $("#admin-current-values").innerHTML=row?fields.map(([label,key])=>`<dt>${label}</dt><dd>${row[key]||"—"}</dd>`).join(""):`<dt>Seleção</dt><dd>Nenhum vínculo encontrado.</dd>`;
}
function renderAdminScopeHint(){
 const values=$("#admin-current-values");
 if(!values||$("#admin-operation")?.value!=="disable")return;
 const scope=$("#admin-deactivation-scope")?.value||"component";
 const macro=$("#admin-source-macro")?.value||"";
 const policy=$("#admin-source-policy")?.value||"";
 const row=eapRows[adminEditIndex];
 const hints={
  component:row?`Será solicitado o encerramento do componente "${row.comp2024}".`:"Selecione uma linha/componente.",
  policy:policy?`Será solicitado o encerramento da política "${policy}", apenas se não houver componente ativo.`:"Selecione uma política.",
  macro:macro?`Será solicitado o encerramento da macropolítica "${macro}", apenas se não houver política ativa.`:"Selecione uma macropolítica."
 };
 if(!values.querySelector(".scope-hint"))values.insertAdjacentHTML("beforeend",`<dt>Escopo</dt><dd class="scope-hint"></dd>`);
 values.querySelector(".scope-hint").textContent=hints[scope]||"";
}
function clearAdminProposal(){
 ["admin-macro","admin-macro-code","admin-axis","admin-axis-code","admin-policy","admin-policy-code","admin-pillar","admin-pillar-code","admin-pee","admin-justification"].forEach(id=>{$("#"+id).value=""});
 $("#admin-process").value="";
 $("#admin-components-list").innerHTML=`<label><span>Novo componente 1</span><input class="admin-component-input" placeholder="Ex.: Aquisições estratégicas"></label>`;
 setAdminProposalDisabled(($("#admin-operation")?.value||"include").startsWith("disable"));
}
function addAdminComponentLine(){
 const count=document.querySelectorAll(".admin-component-input").length+1;
 $("#admin-components-list").insertAdjacentHTML("beforeend",`<label><span>Novo componente ${count}</span><input class="admin-component-input" placeholder="Informe o novo componente"></label>`);
 setAdminProposalDisabled(($("#admin-operation")?.value||"include").startsWith("disable"));
}
function readAdminValue(id){return ($("#"+id)?.value||"").trim()}
function saveEapAdminChange(){
 const operation=$("#admin-operation").value;
 if(operation==="disable"){requestUnifiedDeletion();return}
 if(operation!=="include"&&adminEditIndex<0){toast("Selecione um vínculo atual antes de salvar.");return}
 const original=eapRows[adminEditIndex];
 if(operation==="change"&&isPendingDelete(original)){toast("Este vínculo possui exclusão pendente. Aprove ou reverta a solicitação antes de alterar.");return}
 const base={
  process:readAdminValue("admin-process"),
  macro:readAdminValue("admin-macro"),
  macroCode:readAdminValue("admin-macro-code"),
  axis:readAdminValue("admin-axis"),
  axisCode:readAdminValue("admin-axis-code"),
  pillar:readAdminValue("admin-pillar"),
  pillarCode:readAdminValue("admin-pillar-code"),
  pee:readAdminValue("admin-pee"),
  policy:readAdminValue("admin-policy"),
  policyCode:readAdminValue("admin-policy-code")
 };
 const justification=readAdminValue("admin-justification");
 const components=[...document.querySelectorAll(".admin-component-input")].map(input=>input.value.trim());
 const missing=Object.entries(base).filter(([,v])=>!v).map(([k])=>k);
 if(missing.length||!justification||!components.length||components.some(v=>!v)){
  toast("Preencha todos os campos da proposta, todos os novos componentes e a justificativa administrativa.");
  return;
 }
 const stamp=Date.now();
 const newRows=components.map((component,index)=>({
  ...base,
  id:`admin-${stamp}-${index+1}`,
  comp2022:original?.comp2022||"(novo vínculo administrativo)",
  comp2023:original?.comp2023||"(novo vínculo administrativo)",
  comp2024:component,
  justification
 }));
 if(operation==="change"&&adminEditIndex>=0)eapRows.splice(adminEditIndex,1,...newRows);
 else eapRows.unshift(...newRows);
 persistEapRows();
 $("#eap-process").value=base.process;
 updateEapMacroOptions();
 $("#eap-macro").value=base.macro;
 updateEapPolicyOptions();
 $("#eap-policy").value=base.policy;
 renderEapTable();
 renderMacros();
 renderPendingReview();
 closeAdminPanel();
 showStructureView("chain");
 toast(`${newRows.length} vínculo(s) de "${base.policy}" incluído(s) localmente e refletido(s) na cadeia estratégica.`);
}
function requireSecondDeleteConfirmation(token,message){
 const save=$("#eap-admin-save");
 if(adminConfirmToken!==token){
  adminConfirmToken=token;
  if(save)save.textContent="Tenho certeza — registrar pendência";
  toast(message);
  return false;
 }
 adminConfirmToken=null;
 return true;
}
function requireSecondStrategyConfirmation(token,message){
 const save=$("#strategy-save");
 if(adminConfirmToken!==token){
  adminConfirmToken=token;
  if(save)save.textContent="Tenho certeza — registrar pendência";
  toast(message);
  return false;
 }
 adminConfirmToken=null;
 if(save)save.textContent="Salvar elementos estratégicos";
 return true;
}
function requestStrategicChange(data,label){
 if(Object.values(data).some(v=>!v)){toast("Preencha todos os elementos estratégicos antes de salvar.");return}
 const pending=loadPendingStrategicChange();
 if(pending){toast("Já existe alteração estratégica pendente. Aprove ou reverta antes de registrar outra.");return}
 if(!requireSecondStrategyConfirmation("strategy-change",`Tem certeza que deseja solicitar ${label}? Clique novamente para registrar como pendente de revisão.`))return;
 savePendingStrategicChange({
  type:"strategy",
  requestedBy:currentUser.name,
  requestedAt:new Date().toISOString(),
  label,
  data,
  previous:loadStrategicElements()
 });
 renderPendingReview();
 closeAdminPanel();
 showStructureView("eap");
 showEapSubView("table");
 toast("Alteração dos elementos estratégicos registrada como pendente para outro administrador.");
}
function requireSecondSupportTextConfirmation(token,message){
 const save=$("#support-text-save");
 if(adminConfirmToken!==token){
  adminConfirmToken=token;
  if(save)save.textContent="Tenho certeza — registrar pendência";
  toast(message);
  return false;
 }
 adminConfirmToken=null;
 if(save)save.textContent="Salvar textos de apoio e visão gerencial";
 return true;
}
function requestSupportTextChange(data,label){
 const justification=readAdminValue("support-text-justification");
 if(Object.values(data).some(v=>!v)){toast("Preencha todos os textos de apoio e visão gerencial antes de salvar.");return}
 if(!justification){toast("Informe a justificativa administrativa para solicitar a alteração dos textos de apoio.");return}
 const pending=loadPendingSupportTextChange();
 if(pending){toast("Já existe alteração de textos de apoio pendente. Aprove ou reverta antes de registrar outra.");return}
 if(!requireSecondSupportTextConfirmation("support-text-change",`Tem certeza que deseja solicitar ${label}? Clique novamente para registrar como pendente de revisão.`))return;
 savePendingSupportTextChange({
  type:"supportTexts",
  requestedBy:currentUser.name,
  requestedAt:new Date().toISOString(),
  label,
  justification,
  data,
  previous:loadSupportTexts()
 });
 renderPendingReview();
 closeAdminPanel();
 showStructureView("eap");
 showEapSubView("map");
 toast("Alteração dos textos de apoio e visão gerencial registrada como pendente para outro administrador.");
}
function requestUnifiedDeletion(){
 const scope=$("#admin-deactivation-scope")?.value||"component";
 if(scope==="macro"){requestMacroDeletionIfSafe();return}
 if(scope==="policy"){requestPolicyDeletionIfSafe();return}
 if(adminEditIndex<0){toast("Selecione uma linha/componente antes de solicitar a desativação.");return}
 requestComponentDeletion(eapRows[adminEditIndex]);
}
function requestComponentDeletion(original){
 const justification=readAdminValue("admin-justification");
 if(!original||!justification){toast("Informe a justificativa administrativa para solicitar a desativação do componente.");return}
 if(original.process==="Histórico"){toast("Este componente já está em histórico.");return}
 if(isPendingDelete(original)){toast("Este componente já possui exclusão pendente de revisão.");return}
 const token=`component:${adminEditIndex}:${original.id||original.comp2024}`;
 if(!requireSecondDeleteConfirmation(token,`Tem certeza que deseja excluir este componente da política "${original.policy}"? Clique novamente para registrar como pendente de revisão.`))return;
 eapRows[adminEditIndex]={...original,_status:"pending-delete",_request:{
  type:"component",
  requestedBy:currentUser.name,
  requestedAt:new Date().toISOString(),
  justification,
  macro:original.macro,
  policy:original.policy,
  component:original.comp2024
 }};
 persistAndRefreshEap();
 const remainingPolicy=activeRowsForPolicy(original.macro,original.policy).length;
 const remainingMacro=activeRowsForMacro(original.macro).length;
 closeAdminPanel();
 showStructureView("eap");
 showEapSubView("table");
 toast(remainingPolicy?`Pedido pendente registrado. A política "${original.policy}" ainda possui ${remainingPolicy} componente(s) ativo(s).`:`Pedido pendente registrado. A política "${original.policy}" não possui mais componentes ativos.`);
 if(!remainingMacro)setTimeout(()=>toast(`A macropolítica "${original.macro}" não possui mais política com componente ativo e pode ser solicitada para desativação.`),900);
}
function requestPolicyDeletionIfSafe(){
 const macro=$("#admin-source-macro")?.value||"";
 const policy=$("#admin-source-policy")?.value||"";
 const justification=readAdminValue("admin-justification");
 if(!macro||!policy){toast("Selecione a macropolítica e a política que deseja solicitar para desativação.");return}
 if(!justification){toast("Informe a justificativa administrativa para solicitar a desativação da política.");return}
 const related=currentRowsForPolicy(macro,policy);
 if(!related.length){toast(`Não há vínculo atual associado à política "${policy}".`);return}
 if(related.some(r=>r._policyStatus==="pending-delete")){toast(`A política "${policy}" já possui desativação pendente de revisão.`);return}
 const active=activeRowsForPolicy(macro,policy);
 if(active.length){
  const components=active.slice(0,5).map(r=>r.comp2024).join(", ");
  toast(`Segurança ativada: a política "${policy}" ainda possui ${active.length} componente(s) ativo(s). Desative primeiro os componentes, um a um. ${components?`Ex.: ${components}`:""}`);
  return;
 }
 const token=`policy:${macro}:${policy}`;
 if(!requireSecondDeleteConfirmation(token,`Tem certeza que deseja excluir a política "${policy}"? Clique novamente para registrar como pendente de revisão.`))return;
 eapRows=eapRows.map(r=>r.macro===macro&&r.policy===policy&&r.process!=="Histórico"?{...r,_policyStatus:"pending-delete",_policyRequest:{
  type:"policy",
  requestedBy:currentUser.name,
  requestedAt:new Date().toISOString(),
  justification,
  macro,
  policy
 }}:r);
 persistAndRefreshEap();
 closeAdminPanel();
 showStructureView("eap");
 showEapSubView("table");
 toast(`Pedido de desativação da política "${policy}" registrado como pendente para outro administrador.`);
}
function requestMacroDeletionIfSafe(){
 const macro=$("#admin-source-macro")?.value||"";
 const justification=readAdminValue("admin-justification");
 if(!macro){toast("Selecione a macropolítica que deseja solicitar para desativação.");return}
 if(!justification){toast("Informe a justificativa administrativa para solicitar a desativação da macropolítica.");return}
 const related=currentRowsForMacro(macro);
 if(!related.length){toast(`Não há vínculo atual ou pendente associado à macropolítica "${macro}".`);return}
 const activePolicies=[...new Set(related.filter(r=>r._policyStatus!=="pending-delete").map(r=>r.policy).filter(Boolean))];
 if(activePolicies.length){
  toast(`Segurança ativada: a macropolítica "${macro}" ainda possui ${activePolicies.length} política(s) ativa(s). Solicite primeiro a desativação das políticas, uma a uma. Ex.: ${activePolicies.slice(0,5).join(", ")}`);
  return;
 }
 const token=`macro:${macro}`;
 if(!requireSecondDeleteConfirmation(token,`Tem certeza que deseja excluir a macropolítica "${macro}"? Clique novamente para registrar como pendente de revisão.`))return;
 eapRows=eapRows.map(r=>r.macro===macro&&r.process!=="Histórico"?{...r,_macroStatus:"pending-delete",_macroRequest:{
  type:"macro",
  requestedBy:currentUser.name,
  requestedAt:new Date().toISOString(),
  justification,
  macro
 }}:r);
 persistAndRefreshEap();
 closeAdminPanel();
 showStructureView("eap");
 showEapSubView("table");
 toast(`Pedido de desativação da macropolítica "${macro}" registrado como pendente para outro administrador.`);
}
function persistAndRefreshEap(){
 persistEapRows();
 updateEapMacroOptions();
 updateEapPolicyOptions();
 renderEapTable();
 renderMacros();
 renderPendingReview();
}
function refreshSupportTextViews(){
 if(!$("#eap-map-panel")?.classList.contains("hidden"))renderEapMap();
 const concept=$("#concept-panel");
 if(concept&&!concept.classList.contains("hidden")){
  const active=$("#concept-strategy")?.classList.contains("active")?"strategy":$("#concept-process")?.classList.contains("active")?"process":"";
  if(active)showChainSubView(active);
 }
}
function pendingItems(){
 const items=[];
 const strategicPending=loadPendingStrategicChange();
 if(strategicPending)items.push({kind:"strategy",row:strategicPending,key:"strategy",title:"Elementos estratégicos",detail:"Identidade organizacional, governança e riscos aguardando aprovação"});
 const supportTextPending=loadPendingSupportTextChange();
 if(supportTextPending)items.push({kind:"supportTexts",row:supportTextPending,key:"supportTexts",title:"Textos de apoio e visão gerencial",detail:"Mapa EAP, visões estratégica/processual e orientações aguardando aprovação"});
 eapRows.forEach((r,i)=>{
  if(r._status==="pending-delete")items.push({kind:"component",index:i,row:r,key:`component-${i}`,title:`Componente: ${r.comp2024}`,detail:`${r.macro} · ${r.policy}`});
 });
 const policyKeys=new Set();
 eapRows.filter(r=>r._policyStatus==="pending-delete").forEach(r=>{
  const key=`${r.macro}|${r.policy}`;
  if(policyKeys.has(key))return;
  policyKeys.add(key);
  const rows=eapRows.filter(x=>x.macro===r.macro&&x.policy===r.policy&&x._policyStatus==="pending-delete");
  items.push({kind:"policy",macro:r.macro,policy:r.policy,key:`policy-${slugCode(key)}`,row:r,title:`Política: ${r.policy}`,detail:`${r.macro} · ${rows.length} componente(s) aguardando revisão da política`});
 });
 [...new Set(eapRows.filter(r=>r._macroStatus==="pending-delete").map(r=>r.macro))].forEach(macro=>{
  const rows=eapRows.filter(r=>r.macro===macro&&r._macroStatus==="pending-delete");
  if(rows.length)items.push({kind:"macro",macro,key:`macro-${slugCode(macro)}`,row:rows[0],title:`Macropolítica: ${macro}`,detail:`${rows.length} vínculo(s) aguardando revisão final`});
 });
 return items;
}
function renderPendingReview(){
 const panel=$("#eap-pending-panel");
 if(!panel)return;
 const items=pendingItems();
 panel.classList.toggle("hidden",!items.length);
 if(!items.length){panel.innerHTML="";return}
 panel.innerHTML=`<div class="pending-head"><div><span class="eyebrow">REVISÃO ADMINISTRATIVA</span><h3>Solicitações pendentes</h3><p>As solicitações abaixo precisam ser confirmadas por outro administrador ou revertidas com registro de auditoria.</p></div><label><span>Administrador revisor</span><select id="admin-reviewer"><option value="">Selecione outro administrador</option><option>Harley Rafael Leopoldo Pereira</option><option>Maria Aparecida da Silva</option><option>Administrador substituto</option></select></label></div><div class="pending-list">${items.map(item=>`<article class="pending-card"><div><strong>${item.title}</strong><small>${item.detail}</small><em>Solicitado por: ${(item.row._request||item.row._policyRequest||item.row._macroRequest||item.row||{}).requestedBy||"—"}</em></div><div><button class="secondary-button pending-revert" data-kind="${item.kind}" data-index="${item.index??""}" data-macro="${item.macro||""}" data-policy="${item.policy||""}" type="button">Reverter</button><button class="danger-button pending-approve" data-kind="${item.kind}" data-index="${item.index??""}" data-macro="${item.macro||""}" data-policy="${item.policy||""}" type="button">${["strategy","supportTexts"].includes(item.kind)?"Aprovar alteração":"Aprovar exclusão"}</button></div></article>`).join("")}</div>`;
 panel.querySelectorAll(".pending-approve").forEach(btn=>btn.onclick=()=>approvePendingDeletion(btn.dataset));
 panel.querySelectorAll(".pending-revert").forEach(btn=>btn.onclick=()=>revertPendingDeletion(btn.dataset));
}
function selectedReviewer(){
 const reviewer=$("#admin-reviewer")?.value||"";
 if(!reviewer){toast("Selecione outro administrador para revisar a operação.");return ""}
 if(reviewer===currentUser.name){toast("O administrador solicitante não pode aprovar a própria exclusão.");return ""}
 return reviewer;
}
function approvePendingDeletion(data){
 const reviewer=selectedReviewer();
 if(!reviewer)return;
 const stamp=new Date().toISOString();
 if(data.kind==="strategy"){
  const pending=loadPendingStrategicChange();
  if(!pending){toast("Não há alteração estratégica pendente.");return}
  saveStrategicElements({...pending.data,_audit:{requestedBy:pending.requestedBy,requestedAt:pending.requestedAt,approvedBy:reviewer,approvedAt:stamp}});
  savePendingStrategicChange(null);
  renderStrategicElements();
  populateStrategicEditor();
  renderPendingReview();
  toast(`Alteração dos elementos estratégicos aprovada por ${reviewer}.`);
  return;
 }
 if(data.kind==="supportTexts"){
  const pending=loadPendingSupportTextChange();
  if(!pending){toast("Não há alteração de textos de apoio pendente.");return}
  saveSupportTexts({...pending.data,_audit:{requestedBy:pending.requestedBy,requestedAt:pending.requestedAt,approvedBy:reviewer,approvedAt:stamp,justification:pending.justification}});
  savePendingSupportTextChange(null);
  populateSupportTextEditor();
  renderPendingReview();
  refreshSupportTextViews();
  toast(`Alteração dos textos de apoio e visão gerencial aprovada por ${reviewer}.`);
  return;
 }
 if(data.kind==="macro"){
  const macro=data.macro;
  eapRows=eapRows.map(r=>r.macro===macro&&r._macroStatus==="pending-delete"?moveRowToHistory(r,reviewer,stamp,"macro"):r);
  persistAndRefreshEap();
  toast(`Exclusão da macropolítica "${macro}" aprovada por ${reviewer}.`);
  return;
 }
 if(data.kind==="policy"){
  const {macro,policy}=data;
  eapRows=eapRows.map(r=>r.macro===macro&&r.policy===policy&&r._policyStatus==="pending-delete"?moveRowToHistory(r,reviewer,stamp,"policy"):r);
  persistAndRefreshEap();
  toast(`Exclusão da política "${policy}" aprovada por ${reviewer}.`);
  return;
 }
 const index=Number(data.index);
 if(!Number.isFinite(index)||!eapRows[index])return;
 eapRows[index]=moveRowToHistory(eapRows[index],reviewer,stamp,"component");
 persistAndRefreshEap();
 toast("Exclusão do componente aprovada e enviada ao histórico.");
}
function revertPendingDeletion(data){
 const reviewer=selectedReviewer();
 if(!reviewer)return;
 const stamp=new Date().toISOString();
 if(data.kind==="strategy"){
  const pending=loadPendingStrategicChange();
  if(!pending){toast("Não há alteração estratégica pendente.");return}
  savePendingStrategicChange(null);
  populateStrategicEditor();
  renderPendingReview();
  toast(`Pedido de alteração dos elementos estratégicos revertido por ${reviewer}.`);
  return;
 }
 if(data.kind==="supportTexts"){
  const pending=loadPendingSupportTextChange();
  if(!pending){toast("Não há alteração de textos de apoio pendente.");return}
  savePendingSupportTextChange(null);
  populateSupportTextEditor();
  renderPendingReview();
  toast(`Pedido de alteração dos textos de apoio e visão gerencial revertido por ${reviewer}.`);
  return;
 }
 if(data.kind==="macro"){
  const macro=data.macro;
  eapRows=eapRows.map(r=>r.macro===macro&&r._macroStatus==="pending-delete"?{...r,_macroStatus:"reverted",_macroRevertedBy:reviewer,_macroRevertedAt:stamp,_macroStatus:null,_macroRequest:null}:r);
  persistAndRefreshEap();
  toast(`Pedido de exclusão da macropolítica "${macro}" revertido por ${reviewer}.`);
  return;
 }
 if(data.kind==="policy"){
  const {macro,policy}=data;
  eapRows=eapRows.map(r=>r.macro===macro&&r.policy===policy&&r._policyStatus==="pending-delete"?{...r,_policyStatus:null,_policyRequest:null,_policyRevertedBy:reviewer,_policyRevertedAt:stamp}:r);
  persistAndRefreshEap();
  toast(`Pedido de exclusão da política "${policy}" revertido por ${reviewer}.`);
  return;
 }
 const index=Number(data.index);
 if(!Number.isFinite(index)||!eapRows[index])return;
 eapRows[index]={...eapRows[index],_status:null,_request:null,_revertedBy:reviewer,_revertedAt:stamp};
 persistAndRefreshEap();
 toast("Pedido de exclusão do componente revertido.");
}
function moveRowToHistory(row,reviewer,stamp,approvalType){
 return {...row,process:"Histórico",macro:"(sem macropolítica atual)",macroCode:"",axis:"(sem eixo atual)",axisCode:"",policyCode:row.policyCode||"",comp2024:row.comp2024||"(sem componente 2024)",_status:null,_policyStatus:null,_macroStatus:null,_approvedBy:reviewer,_approvedAt:stamp,_approvalType:approvalType,_audit:{request:row._request||row._policyRequest||row._macroRequest,approvedBy:reviewer,approvedAt:stamp,approvalType},_request:null,_policyRequest:null,_macroRequest:null};
}
function updateEapMacroOptions(){
 const process=$("#eap-process").value;
 const selected=$("#eap-macro").value;
 const policy=$("#eap-policy").value;
 const source=(eapRows||[]).filter(r=>(!process||r.process===process)&&(!policy||r.policy===policy));
 const macros=[...new Set(source.map(r=>r.macro).filter(Boolean))];
 $("#eap-macro").innerHTML='<option value="">Todas</option>'+macros.map(m=>`<option>${m}</option>`).join("");
 if(selected&&macros.includes(selected))$("#eap-macro").value=selected;
 else $("#eap-macro").value="";
}
function updateEapPolicyOptions(){
 const process=$("#eap-process").value;
 const macro=$("#eap-macro").value;
 const selected=$("#eap-policy").value;
 const source=(eapRows||[]).filter(r=>(!process||r.process===process)&&(!macro||r.macro===macro));
 const policies=[...new Set(source.map(r=>r.policy).filter(Boolean))];
 $("#eap-policy").innerHTML='<option value="">Todas</option>'+policies.map(p=>`<option>${p}</option>`).join("");
 if(selected&&policies.includes(selected))$("#eap-policy").value=selected;
 else $("#eap-policy").value="";
}
function syncEapProcessFromMacro(){
 const macro=$("#eap-macro").value;
 if(!macro)return;
 const row=(eapRows||[]).find(r=>r.macro===macro);
 if(row)$("#eap-process").value=row.process||"";
}
function syncEapFromPolicy(){
 const policy=$("#eap-policy").value;
 if(!policy)return;
 const currentProcess=$("#eap-process").value;
 const currentMacro=$("#eap-macro").value;
 const row=(eapRows||[]).find(r=>r.policy===policy&&(!currentProcess||r.process===currentProcess)&&(!currentMacro||r.macro===currentMacro))||(eapRows||[]).find(r=>r.policy===policy);
 if(row){
  $("#eap-process").value=row.process||"";
  $("#eap-macro").value=row.macro||"";
 }
}
function showStructureView(view){
 const eap=view==="eap";
 $("#chain-tab").classList.toggle("active",!eap);
 $("#eap-tab").classList.toggle("active",eap);
 $("#chain-tools").classList.toggle("hidden",eap);
 $("#eap-tools").classList.toggle("hidden",!eap);
 $("#policy-expansion").classList.add("hidden");
 if(eap)showEapSubView("map");else showChainSubView("map");
}
function showChainSubView(view){
 const map=view==="map";
 $("#chain-map").classList.toggle("active",map);
 $("#chain-panel").classList.toggle("hidden",!map);
 $("#eap-map-panel").classList.add("hidden");
 $("#eap-panel").classList.add("hidden");
 if(map){
  $("#concept-panel").classList.add("hidden");
  $("#concept-panel").dataset.view="";
  $("#concept-panel").innerHTML="";
  document.querySelectorAll("#chain-tools .concept-button").forEach(b=>b.classList.remove("active"));
  $("#chain-map").classList.add("active");
 }
}
function showEapSubView(view){
 const table=view==="table";
 $("#eap-map-tab").classList.toggle("active",!table);
 $("#eap-table-tab").classList.toggle("active",table);
 $("#chain-panel").classList.add("hidden");
 $("#concept-panel").classList.add("hidden");
 $("#concept-panel").dataset.view="";
 $("#concept-panel").innerHTML="";
 $("#eap-map-panel").classList.toggle("hidden",table);
 $("#eap-panel").classList.toggle("hidden",!table);
 if(!table)renderEapMap();
}
function renderConceptPanel(type){
 const panel=$("#concept-panel");
 $("#chain-panel").classList.add("hidden");
 $("#eap-map-panel").classList.add("hidden");
 $("#eap-panel").classList.add("hidden");
 document.querySelectorAll("#chain-tools .concept-button").forEach(b=>b.classList.remove("active"));
 panel.dataset.view=type;
 panel.classList.remove("hidden");
 $(`#concept-${type}`).classList.add("active");
 panel.innerHTML=type==="strategy"?strategicConceptMarkup():processConceptMarkup();
 syncThemedSvgs(panel);
 panel.querySelector(".close-expansion").onclick=()=>showChainSubView("map");
 panel.querySelector("#export-process-diagram")?.addEventListener("click",exportProcessDiagramPng);
 panel.querySelector("#export-strategy-diagram")?.addEventListener("click",exportStrategyDiagramPng);
 panel.querySelectorAll(".process-summary-card").forEach(btn=>btn.onclick=()=>selectProcessDetail(btn.dataset.process));
 panel.scrollIntoView({behavior:"smooth",block:"nearest"});
}
function activeEapRows(){return (eapRows||[]).filter(r=>r.process!=="Histórico"&&!isPendingDelete(r))}
function themedSvgSrc(src){return document.body.classList.contains("dark")?src.replace(/\.svg$/,".dark.svg"):src}
function diagramImg(id,src,alt){return `<img id="${id}" class="integration-svg" src="${themedSvgSrc(src)}" data-light-src="${src}" data-dark-src="${src.replace(/\.svg$/,".dark.svg")}" alt="${alt}">`}
function syncThemedSvgs(root=document){
 const dark=document.body.classList.contains("dark");
 root.querySelectorAll?.(".integration-svg[data-light-src][data-dark-src]").forEach(img=>{
  const target=dark?img.dataset.darkSrc:img.dataset.lightSrc;
  if(target&&img.getAttribute("src")!==target)img.setAttribute("src",target);
 });
}
function renderEapMap(){
 const rows=activeEapRows();
 const macroCount=new Set(rows.map(r=>r.macro).filter(Boolean)).size;
 const policyCount=new Set(rows.map(r=>r.policy).filter(Boolean)).size;
 const componentCount=new Set(rows.map(r=>r.comp2024).filter(Boolean)).size;
 const pillarCount=new Set(rows.map(r=>r.pillar).filter(Boolean)).size;
 const panel=$("#eap-map-panel");
 if(!panel)return;
 const nodes=[1,2,3,4,5,6].map(n=>eapMapNode(n,textParam(`eap.flow.${n}.title`),textParam(`eap.flow.${n}.summary`),textParam(`eap.flow.${n}.detail`))).join("<i>→</i>");
 panel.innerHTML=`<div class="eap-map-card"><div class="concept-head compact-head"><div><span class="eyebrow">${textParam("eap.map.eyebrow")}</span><h3>${textParam("eap.map.title")}</h3><p>${textParam("eap.map.description")}</p></div></div><div class="eap-map-stats"><span><strong>${macroCount}</strong> macropolíticas ativas</span><span><strong>${policyCount}</strong> políticas ativas</span><span><strong>${componentCount}</strong> componentes ativos</span><span><strong>${pillarCount}</strong> pilares vinculados</span></div><div class="eap-map-flow" aria-label="Fluxo conceitual da EAP">${nodes}</div><div class="vector-diagram-card eap-integration-card"><div class="vector-head"><span>EAP, PTA, Entregas MT e processos</span><button id="export-eap-integration-diagram" class="secondary-button" type="button">Exportar diagrama PNG</button></div><button id="toggle-eap-diagram-size" class="diagram-float-toggle" type="button">↕ Ampliar mapa</button>${diagramImg("eap-integration-svg","assets/diagrama-eap-integracao-pta-entregas-processos.svg","Diagrama conceitual da EAP integrada ao PTA, Sistema Entregas MT, processos, orçamento e execução")}</div><div class="eap-map-branch"><section><strong>${textParam("eap.layer.orientation.title")}</strong><p>${textParam("eap.layer.orientation.text")}</p></section><section><strong>${textParam("eap.layer.governance.title")}</strong><p>${textParam("eap.layer.governance.text")}</p></section><section><strong>${textParam("eap.layer.integration.title")}</strong><p>${textParam("eap.layer.integration.text")}</p></section></div></div>`;
 syncThemedSvgs(panel);
 panel.querySelectorAll(".eap-map-node").forEach(btn=>btn.onclick=()=>btn.classList.toggle("open"));
 panel.querySelector("#export-eap-integration-diagram")?.addEventListener("click",exportEapIntegrationDiagramPng);
 panel.querySelector("#toggle-eap-diagram-size")?.addEventListener("click",toggleEapDiagramSize);
}
function eapMapNode(num,title,summary,detail){return `<button class="eap-map-node" type="button"><span>${num}</span><strong>${title}</strong><p>${summary}</p><em>${detail}</em></button>`}
function eapLayer(kind,num,title,summary,detail){return `<button class="eap-layer-node ${kind}" type="button"><span>${num}</span><strong>${title}</strong><p>${summary}</p><em>${detail}</em></button>`}
function eapConnector(text){return `<div class="eap-layer-connector">${text}</div>`}
function strategicConceptMarkup(){
 return `<div class="concept-head"><div><span class="eyebrow">${textParam("strategy.view.eyebrow")}</span><h3>${textParam("strategy.view.title")}</h3><p>${textParam("strategy.view.description")}</p></div><div class="concept-actions"><button id="export-strategy-diagram" class="secondary-button" type="button">Exportar diagrama PNG</button><button class="close-expansion" type="button">×</button></div></div><div class="vector-diagram-card strategic-svg-card"><div class="vector-head"><span>${textParam("strategy.view.diagramTitle")}</span></div>${diagramImg("strategy-diagram-svg","assets/diagrama-cadeia-estrategica.svg","Diagrama da cadeia estratégica de programação PTA")}</div>`;
}
function processConceptMarkup(){
 const info=processInfo();
 const examples=textListParam("process.examples.items").map(item=>`<li>${item}</li>`).join("");
 return `<div class="concept-head"><div><span class="eyebrow">${textParam("process.view.eyebrow")}</span><h3>${textParam("process.view.title")}</h3><p>${textParam("process.view.description")}</p></div><div class="concept-actions"><button id="export-process-diagram" class="secondary-button" type="button">Exportar diagrama PNG</button><button class="close-expansion" type="button">×</button></div></div><div class="concept-diagram process-view"><div class="process-summary-grid">${processSummaryCard("support",info.support.title,textParam("process.support.summary"))}${processSummaryCard("managerial",info.managerial.title,textParam("process.managerial.summary"))}${processSummaryCard("finalistic",info.finalistic.title,textParam("process.finalistic.summary"))}</div><div id="process-detail-panel" class="hidden"></div><div class="hierarchy-note"><strong>${textParam("process.chain.label")}</strong><span>${textParam("process.chain.text")}</span></div><div class="vector-diagram-card"><div class="vector-head"><span>${textParam("process.diagram.title")}</span></div>${processIntegrationSvg()}</div><div class="concept-examples"><span>${textParam("process.examples.title")}</span><ul>${examples}</ul></div></div>`;
}
function processSummaryCard(key,title,text){
 return `<button class="process-summary-card" data-process="${key}" type="button"><strong>${title}</strong><span>${text}</span></button>`;
}
function processInfo(){
 return {
  support:{title:"Processos de suporte",items:textListParam("process.support.items"),value:textParam("process.support.value"),clients:textParam("process.support.clients")},
  managerial:{title:"Processos gerenciais",items:textListParam("process.managerial.items"),value:textParam("process.managerial.value"),clients:textParam("process.managerial.clients")},
  finalistic:{title:"Processos finalísticos",items:textListParam("process.finalistic.items"),value:textParam("process.finalistic.value"),clients:textParam("process.finalistic.clients")}
 };
}
function processDetailMarkup(key){
 const item=processInfo()[key]||processInfo().support;
 return `<div class="process-detail"><section><h4>${item.title}</h4><div class="process-lines compact">${item.items.map(p=>`<span>${p}</span>`).join("")}</div></section><aside><article><strong>Proposta de valor</strong><p>${item.value}</p></article><article><strong>Clientes</strong><p>${item.clients}</p></article></aside></div>`;
}
function selectProcessDetail(key){
 const selected=document.querySelector(`.process-summary-card[data-process="${key}"]`);
 const close=selected?.classList.contains("active");
 document.querySelectorAll(".process-summary-card").forEach(btn=>btn.classList.remove("active"));
 const panel=$("#process-detail-panel");
 if(!panel)return;
 if(close){panel.classList.add("hidden");panel.innerHTML="";return}
 selected?.classList.add("active");
 panel.classList.remove("hidden");
 panel.innerHTML=processDetailMarkup(key);
}
function processIntegrationSvg(){
 return diagramImg("process-integration-svg","assets/diagrama-integracao-cadeias.svg","Diagrama de integração entre cadeia estratégica de programação PTA e cadeia de valor organizacional/processual");
}
function exportProcessDiagramPng(){
 const diagram=$("#process-integration-svg");
 exportDiagramImage(diagram,"spo-integracao-cadeia-estrategica-processual.png");
}
function exportStrategyDiagramPng(){
 const diagram=$("#strategy-diagram-svg");
 exportDiagramImage(diagram,"spo-cadeia-estrategica-programacao-pta.png");
}
function exportEapIntegrationDiagramPng(){
 const diagram=$("#eap-integration-svg");
 exportDiagramImage(diagram,"spo-eap-integracao-pta-entregas-processos.png");
}
function toggleEapDiagramSize(){
 const card=document.querySelector(".eap-integration-card");
 const button=$("#toggle-eap-diagram-size");
 if(!card||!button)return;
 const expanded=card.classList.toggle("diagram-expanded");
 button.textContent=expanded?"↕ Reduzir mapa":"↕ Ampliar mapa";
 if(expanded)card.scrollIntoView({behavior:"smooth",block:"nearest"});
}
function exportDiagramImage(diagram,filename){
 if(!diagram)return;
 const url=diagram.getAttribute("src");
 const img=new Image();
 img.onload=()=>{
  const canvas=document.createElement("canvas");
  canvas.width=(img.naturalWidth||1194)*2;canvas.height=(img.naturalHeight||405)*2;
  const ctx=canvas.getContext("2d");
  ctx.fillStyle=url.includes(".dark.svg")?"#10191b":"#ffffff";ctx.fillRect(0,0,canvas.width,canvas.height);
  ctx.drawImage(img,0,0,canvas.width,canvas.height);
  const a=document.createElement("a");
  a.download=filename;
  a.href=canvas.toDataURL("image/png");
  a.click();
 };
 img.src=url;
}
function eapRowClass(row){return row.process==="Histórico"?"historic-row":isPendingDelete(row)?"pending-row":""}
function statusBadges(row){
 const badges=[];
 if(row.process==="Histórico")badges.push(`<span class="status-badge historic">Histórico</span>`);
 if(row._status==="pending-delete")badges.push(`<span class="status-badge pending">Exclusão pendente</span>`);
 if(row._policyStatus==="pending-delete")badges.push(`<span class="status-badge pending">Política pendente</span>`);
 if(row._macroStatus==="pending-delete")badges.push(`<span class="status-badge pending">Macro pendente</span>`);
 return badges.join("");
}
function renderEapTable(){
 const q=($("#eap-search").value||"").toLocaleLowerCase("pt-BR").normalize("NFD").replace(/[\u0300-\u036f]/g,"");
 const process=$("#eap-process").value;
 const macro=$("#eap-macro").value;
 const policy=$("#eap-policy").value;
 let rows=eapRows||[];
 rows=rows.filter(r=>{
  if(process&&r.process!==process)return false;
  if(macro&&r.macro!==macro)return false;
  if(policy&&r.policy!==policy)return false;
  if(!q)return true;
  return Object.values(r).join(" ").toLocaleLowerCase("pt-BR").normalize("NFD").replace(/[\u0300-\u036f]/g,"").includes(q);
 });
 const visible=rows.slice(0,120);
 $("#eap-summary").innerHTML=`<strong>${rows.length}</strong> linha(s) encontradas · <span>${new Set(rows.map(r=>r.macro)).size}</span> macropolítica(s) · <span>${new Set(rows.map(r=>r.axis)).size}</span> eixo(s) · <span>${new Set(rows.map(r=>r.policy)).size}</span> política(s)`;
 $("#eap-body").innerHTML=visible.map(r=>`<tr class="${eapRowClass(r)}"><td><span class="process-pill">${r.process}</span>${statusBadges(r)}</td><td><strong>${r.macro}</strong><small>${r.macroCode}</small></td><td>${r.axis}<small>${r.axisCode}</small></td><td>${r.pillar}<small>Metas ${r.pee}</small></td><td>${r.policy}<small>${r.policyCode}</small></td><td><code>${r.macroCode}</code><code>${r.axisCode}</code><code>${r.pillarCode}</code><code>${r.policyCode}</code></td><td>${r.comp2022}</td><td>${r.comp2023}</td><td>${r.comp2024}</td></tr>`).join("");
 if(rows.length>visible.length)$("#eap-body").insertAdjacentHTML("beforeend",`<tr><td colspan="9" class="table-note">Mostrando 120 de ${rows.length} linhas. Use os filtros para refinar a consulta.</td></tr>`);
}
function macroCard(m,i){
 return `<button class="macro-card" style="--macro-color:${m.color}" data-macro="${i}" type="button"><span class="macro-number">${String(i+1).padStart(2,"0")}</span><strong>${m.name}</strong><small>${new Set(m.axes.map(a=>a[0])).size} eixo(s) de execução →</small></button>`;
}
function expandMacro(index){
 const box=$("#policy-expansion");
 if(!box.classList.contains("hidden")&&box.dataset.macro===String(index)){
  box.classList.add("hidden");
  box.dataset.macro="";
  document.querySelectorAll(".macro-card").forEach(c=>c.classList.remove("active"));
  return;
 }
 state.macro=dynamicMacros[index];
 document.querySelectorAll(".macro-card").forEach((c,i)=>c.classList.toggle("active",i===index));
 box.classList.remove("hidden");
 box.dataset.macro=String(index);
 box.innerHTML=`<div class="expansion-head"><div><span class="eyebrow">MACROPOLÍTICA ${String(index+1).padStart(2,"0")}</span><h3>${state.macro.name}</h3><p>Escolha o eixo de execução. A política PAOE correspondente aparece em cada alternativa. Clique novamente na macropolítica para recolher.</p></div></div><div class="policy-list">${state.macro.axes.map((a,i)=>`<article class="policy-option"><span class="policy-icon">${i+1}</span><div><strong>${a[0]}</strong><span>Política PAOE: ${a[2]} · Pilar: ${a[4]}</span></div><button class="primary-button start-policy" data-axis="${i}" type="button">Programar</button></article>`).join("")}</div>`;
 box.querySelectorAll(".start-policy").forEach(b=>b.onclick=()=>startWizard(+b.dataset.axis));
 box.scrollIntoView({behavior:"smooth",block:"center"});
}
function startWizard(i){
 state.axis=state.macro.axes[i];state.step=0;state.contextGoalsOpen=false;state.contextPolicyOpen=false;state.contextOpenGoals=[];state.subactions=normalizeSubactions(state.subactions);
 $("#value-view").classList.add("hidden");$("#wizard-view").classList.remove("hidden");$("#journey-title").textContent=state.macro.name;
 requestImmersiveMode(true);
 requestContextFocus(true);
 renderWizard();
 if(isEmbeddedMode)window.parent?.postMessage({type:"pta2027-scroll-top"},window.location.origin);else scrollTo({top:0});
 requestEmbeddedLayout();
}
function normalizeSubactions(list){return (list||[]).map(row=>({...row,components:Array.isArray(row.components)?row.components:[],regionalize:!!row.regionalize,regions:Array.isArray(row.regions)?row.regions:[],regionTargets:row.regionTargets||{},entregaMt:{enabled:!!row.entregaMt?.enabled,type:row.entregaMt?.type||"",product:row.entregaMt?.product||"",unit:row.entregaMt?.unit||"",quantity:row.entregaMt?.quantity||"",notes:row.entregaMt?.notes||""}}))}
function normalizeStages(list){return (list||[]).map(row=>({...row,processLink:{enabled:!!row.processLink?.enabled,type:row.processLink?.type||"support",process:row.processLink?.process||"",responsible:row.processLink?.responsible||"",risk:row.processLink?.risk||"",control:row.processLink?.control||"",evidence:row.processLink?.evidence||""}}))}
function stepHasData(i){
 if(i===0)return !!(state.macro&&state.axis);
 if(i===1)return state.regions.length>0;
 if(i===2)return normalizeSubactions(state.subactions).some(r=>(r.text||"").trim()||(r.components||[]).length);
 if(i===3)return normalizeStages(state.stages).some(r=>(r.text||"").trim()||r.processLink?.enabled);
 if(i===4)return !!state.budget;
 if(i===5)return normalizeSubactions(state.subactions).some(r=>(r.text||"").trim())&&!!state.budget;
 return false;
}
function saveCurrentStepInputs(){
 if(state.step===2&&$("#subaction-list"))collectSubactions($("#subaction-list"));
 if(state.step===3&&$("#stage-list"))collectStages($("#stage-list"));
 if(state.step===4&&$("#budget"))state.budget=$("#budget").value;
}
function renderTree(){
 $("#journey-tree").innerHTML=steps.map((s,i)=>{const filled=stepHasData(i);return `<button class="tree-step ${i===state.step?"active":""} ${filled?"complete":"preview"}" data-step="${i}" type="button" aria-label="Ir para ${s[0]}"><i>${filled&&i!==state.step?"✓":i+1}</i><span><b>${s[0]}</b>${s[1]}</span></button>`}).join("");
 document.querySelectorAll(".tree-step").forEach(b=>b.onclick=()=>{saveCurrentStepInputs();state.step=+b.dataset.step;renderWizard();document.querySelector(".workspace")?.scrollIntoView({behavior:"smooth",block:"start"})});
}
function contextGoalCards(){
 const nums=(String(state.axis[6]||"").match(/\d+/g)||[]).map(Number);
 if(!nums.length)return `<div class="context-goals-panel"><p>Sem metas PEE vinculadas a este item.</p></div>`;
 const pillarColor=(pillars.find(p=>String(state.axis[4]||"").includes(p[0]))||[])[2]||"#0f7c68";
 return `<div class="context-goals-panel" style="--active-pillar-color:${pillarColor}" aria-label="Metas e indicadores do PEE vinculados ao pilar">${nums.map(n=>{const open=(state.contextOpenGoals||[]).includes(n);return `<article class="context-goal-stack"><button class="context-goal-card context-goal-toggle ${open?"open":""}" data-goal="${n}" type="button"><span>Meta ${n}</span><p>${peeGoals[n]||"Texto da meta não localizado."}</p><em>${open?"Ocultar indicadores ↑":"Ver indicadores ↓"}</em></button><div class="context-indicator-card ${open?"":"hidden"}" data-context-indicators="${n}"><em>Indicadores da meta ${n}</em>${renderPeeIndicators(n)}</div></article>`}).join("")}</div>`;
}
function contextPolicyComponents(){
 const components=getPolicyComponents();
 const count=components.length;
 const linked=new Set(normalizeSubactions(state.subactions).flatMap(r=>r.components||[]).filter(Boolean));
 return `<div class="context-components-panel" aria-label="Componentes programados da política PAOE"><div class="context-components-head"><strong>${count} componente(s) disponível(is)</strong><span>${linked.size} vinculado(s) nesta programação</span></div>${count?`<ol>${components.map(c=>`<li class="${linked.has(c)?"linked":""}">${c}${linked.has(c)?"<em>Vinculado</em>":""}</li>`).join("")}</ol>`:`<p>Nenhum componente ativo localizado para esta política.</p>`}<p>Esses vínculos permitem contabilizar quais componentes da EAP foram efetivamente materializados em subações/entregas.</p></div>`;
}
function firstProgrammedRegion(){return normalizeSubactions(state.subactions).flatMap(r=>r.regions||[])[0]||state.regions[0]||""}
function renderContext(){
 $("#context-cards").innerHTML=`<div class="context-item"><small>Macropolítica</small><strong>${state.macro.name}</strong></div><div class="context-item"><small>Eixo de execução</small><strong>${state.axis[0]}</strong></div><button class="context-item context-item-button ${state.contextPolicyOpen?"open":""}" id="toggle-context-policy" type="button" aria-expanded="${state.contextPolicyOpen}"><small>Política PAOE</small><strong>${state.axis[2]}</strong><em>${state.contextPolicyOpen?"Ocultar componentes ↑":"Ver componentes programados ↓"}</em></button>${state.contextPolicyOpen?contextPolicyComponents():""}<button class="context-item context-item-button ${state.contextGoalsOpen?"open":""}" id="toggle-context-goals" type="button" aria-expanded="${state.contextGoalsOpen}"><small>Pilar / Metas PEE</small><strong>${state.axis[4]} · ${state.axis[6]}</strong><em>${state.contextGoalsOpen?"Ocultar metas e indicadores ↑":"Ver metas e indicadores ↓"}</em></button>${state.contextGoalsOpen?contextGoalCards():""}`;
 $("#toggle-context-policy").onclick=()=>{state.contextPolicyOpen=!state.contextPolicyOpen;renderContext()};
 $("#toggle-context-goals").onclick=()=>{state.contextGoalsOpen=!state.contextGoalsOpen;renderContext()};
 document.querySelectorAll(".context-goal-toggle").forEach(btn=>btn.onclick=()=>{const n=+btn.dataset.goal;const open=state.contextOpenGoals||[];state.contextOpenGoals=open.includes(n)?open.filter(x=>x!==n):[...open,n];renderContext()});
 $("#planning-key").textContent=`* ${firstProgrammedRegion()||"R___"} * ${state.subfunction}.${state.unit} * ${state.adjunct} * ${state.macro.code} * ${state.axis[5]} * ${state.axis[1]} * ${state.axis[3]} * _ *`;
 $("#quality-region").textContent=regionalizedCount()?"✓":"—";$("#quality-delivery").textContent=state.subactions.some(x=>x.text)?"✓":"—";$("#quality-budget").textContent=+state.budget>0?"✓":"—";
}
function renderWizard(){
 renderTree();renderContext();
 const info=[["Direcionamento estratégico","Confirme a origem estratégica e indique a estrutura responsável pela entrega."],["Regiões e metas físicas","Defina onde o valor público será entregue e qual resultado físico será alcançado."],["Subações e entregas","Descreva as entregas concretas que materializam a política durante o exercício."],["Etapas de execução","Organize os principais marcos necessários para concluir as subações."],["Recursos necessários","Somente agora traduza as entregas em estrutura orçamentária e financeira."],["Revisão integrada","Confira a coerência entre estratégia, entrega, meta, orçamento e chave de planejamento."]][state.step];
 $("#step-kicker").textContent=`ETAPA ${state.step+1} DE ${steps.length}`;$("#step-title").textContent=info[0];$("#step-description").textContent=info[1];$("#progress-value").textContent=`${Math.round((state.step+1)/steps.length*100)}%`;$("#previous-step").disabled=state.step===0;$("#next-step").textContent=state.step===5?"Concluir programação":"Continuar →";$("#step-content").innerHTML=stepMarkup();bindStepEvents();
}
function summary(label,value){return `<div class="summary-item"><small>${label}</small><strong>${value}</strong></div>`}
function validationSummary(label,ok,okText="✓ Validado",warnText="⚠ Verificar"){return `<div class="summary-item ${ok?"valid":"warning"}"><small>${label}</small><strong>${ok?okText:warnText}</strong></div>`}
function validationState(label,stateText,stateClass="valid"){return `<div class="summary-item ${stateClass}"><small>${label}</small><strong>${stateText}</strong></div>`}
function suggestedRegionTarget(index){const values=[1250,980,740,860,1120,910,680,540,790,430,360,300,"Consolidada"];return values[index]??"Base oficial"}
function stepMarkup(){
 if(state.step===0)return `<div class="form-section"><h3>Origem estratégica selecionada</h3><p>Esta relação vem da EAP institucional e não precisa ser redigitada.</p><div class="selection-summary">${summary("Macropolítica",state.macro.name)}${summary("Eixo de execução",state.axis[0])}${summary("Política / Produto PAOE",state.axis[2])}${summary("Pilar e metas PEE",`${state.axis[4]} · Metas ${state.axis[6]}`)}</div></div><div class="form-section"><h3>Responsabilidade e enquadramento</h3><div class="form-grid"><label class="field"><span>Secretaria Adjunta responsável</span><select id="adjunct"><option value="SAIP">SAIP — Impacto Educacional</option><option value="SAGE">SAGE — Gestão Educacional</option><option value="SAAS">SAAS — Administração Sistêmica</option></select></label><label class="field"><span>Unidade gestora</span><select id="unit"><option value="2">0002 — Ensino Fundamental</option><option value="3">0003 — Ensino Médio</option><option value="1">0001 — Administração</option></select></label><label class="field"><span>Subfunção predominante</span><select id="subfunction"><option value="361">361 — Ensino Fundamental</option><option value="362">362 — Ensino Médio</option><option value="366">366 — EJA</option><option value="367">367 — Educação Especial</option><option value="122">122 — Administração Geral</option></select></label><label class="field"><span>Público transversal</span><select><option>Estudantes</option><option>Profissionais da educação</option><option>Comunidade escolar</option><option>Toda a rede</option></select></label></div></div>`;
 if(state.step===1)return `<div class="form-section"><h3>Metas físicas programadas na política PAOE</h3><p>Esta etapa apresenta as metas físicas disponíveis para o produto PAOE selecionado. Selecione uma região de referência. A regionalização múltipla e a geração de registros por região continuam sendo detalhadas dentro da subação/entrega.</p><div class="selection-summary">${summary("Unidade de medida","Definida no produto PAOE")}${summary("Meta física geral","Carregada da base oficial")}${summary("Região de referência",state.regions[0]||"Selecione uma região")}${summary("Regionalização efetiva","Feita dentro da subação")}</div></div><div class="form-section"><h3>Regiões disponíveis</h3><p>Na contabilidade, a programação percorre região → subação → etapas → orçamento, sempre vinculada antes ao produto PAOE.</p><div class="region-readonly-grid selectable">${regions.map((r,i)=>`<label class="${state.regions[0]===r[0]?"selected":""}"><input name="region-reference" type="radio" value="${r[0]}" ${state.regions[0]===r[0]?"checked":""}><strong>${r[0]}</strong><span>${r[1]}</span><small>Meta física: ${suggestedRegionTarget(i)}</small></label>`).join("")}</div></div>`;
 if(state.step===2)return subactionStepMarkup()+`<div class="form-section"><div class="form-grid"><label class="field"><span>Responsável pela subação</span><input placeholder="Nome e unidade setorial"></label><label class="field"><span>Produto da subação</span><input placeholder="Produto ou serviço entregue"></label></div></div>`;
 if(state.step===3)return stageStepMarkup();
 if(state.step===4)return `<div class="form-section"><h3>Estrutura orçamentária</h3><p>A codificação contábil é sugerida a partir das escolhas anteriores e validada pela equipe técnica.</p><div class="form-grid"><label class="field"><span>PAOE sugerida</span><select><option>4172 — Desenvolvimento do Ensino Fundamental</option><option>4174 — Desenvolvimento do Ensino Médio</option><option>2900 — Desenvolvimento da EJA</option><option>2957 — Desenvolvimento da Educação Especial</option></select></label><label class="field"><span>Fonte de recursos</span><select><option>1.500.0000 — Recursos não vinculados</option><option>1.540.0000 — FUNDEB</option><option>1.550.0000 — FNDE</option></select></label><label class="field"><span>Natureza da despesa</span><select><option>3.3.90.39 — Outros Serviços de Terceiros</option><option>3.3.90.30 — Material de Consumo</option><option>4.4.90.51 — Obras e Instalações</option><option>4.4.90.52 — Equipamentos</option></select></label><label class="field"><span>Valor total estimado</span><input id="budget" type="number" min="0" value="${state.budget}" placeholder="0,00"></label><label class="field full"><span>Justificativa do recurso</span><textarea placeholder="Explique como os recursos viabilizam as entregas e metas."></textarea></label></div></div>`;
 return `<div class="form-section"><h3>Síntese da programação</h3><p>O gabarito final será produzido a partir deste encadeamento.</p><div class="review-list"><div class="review-line"><span>Direção estratégica</span><strong>${state.macro.name} → ${state.axis[0]} → ${state.axis[2]}</strong></div><div class="review-line"><span>Pilar / PEE</span><strong>${state.axis[4]} · Metas ${state.axis[6]}</strong></div><div class="review-line"><span>Região de referência</span><strong>${state.regions[0]||"Não informada"}</strong></div><div class="review-line"><span>Regionalização</span><strong>${regionalizationSummary()}</strong></div><div class="review-line"><span>Sistema Entregas MT</span><strong>${entregaMtSummary()}</strong></div><div class="review-line"><span>Subações / etapas</span><strong>${state.subactions.filter(x=>x.text).length} / ${normalizeStages(state.stages).filter(x=>x.text).length}</strong></div><div class="review-line"><span>Processos vinculados</span><strong>${processLinkSummary()}</strong></div><div class="review-line"><span>Componentes vinculados</span><strong>${linkedComponentSummary()}</strong></div><div class="review-line"><span>Orçamento estimado</span><strong>${money(state.budget)}</strong></div><div class="review-line"><span>Chave de planejamento gerada</span><strong>${$("#planning-key").textContent}</strong></div></div></div><div class="form-section"><h3>Validações do SPO</h3><div class="selection-summary validation-grid">${validationSummary("Coerência EAP",true,"✓ Relação válida")}${validationSummary("Chave de planejamento",true,"✓ Estrutura válida")}${validationSummary("Região de referência",!!state.regions[0],"✓ Informada")}${validationSummary("Regionalização por subação",!!regionalizedCount(),"✓ Informada")}${validationSummary("Componentes da entrega",!!linkedComponentCount(),"✓ Vinculados")}${validationSummary("Compatibilidade orçamentária",!!state.budget,"✓ Valor informado")}${entregaMtValidationCard()}${processValidationCard()}</div></div>`;
}
function htmlSafe(value){return String(value??"").replace(/[&<>"']/g,m=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#039;"}[m]))}
function getPolicyComponents(){
 const base=eapRows.length?eapRows:(window.EAP_ROWS||[]);
 const active=base.filter(isActiveEapRow);
 let rows=active.filter(r=>r.macro===state.macro.name&&r.axis===state.axis[0]&&r.policy===state.axis[2]);
 if(!rows.length)rows=active.filter(r=>r.macro===state.macro.name&&r.policy===state.axis[2]);
 return [...new Set(rows.map(r=>r.comp2024).filter(v=>v&&v!=="(sem componente 2024)"))];
}
function linkedComponentCount(){return new Set(normalizeSubactions(state.subactions).flatMap(r=>r.components||[]).filter(Boolean)).size}
function linkedComponentSummary(){
 const count=linkedComponentCount();
 const actions=normalizeSubactions(state.subactions).filter(r=>(r.components||[]).length).length;
 return count?`${count} componente(s) em ${actions} subação(ões)`:"Nenhum componente vinculado";
}
function regionalizedCount(){return normalizeSubactions(state.subactions).filter(r=>r.regionalize&&(r.regions||[]).length).length}
function regionalizationSummary(){const count=regionalizedCount();const regs=new Set(normalizeSubactions(state.subactions).flatMap(r=>r.regions||[]));return count?`${count} subação(ões) regionalizada(s) em ${regs.size} região(ões)`:"Regionalização não detalhada por subação"}
function entregaMtSummary(){const count=normalizeSubactions(state.subactions).filter(r=>r.entregaMt?.enabled).length;return count?`${count} entrega(s) elegível(is) informada(s)`:"Nenhuma entrega MT informada"}
function processLinkSummary(){const count=normalizeStages(state.stages).filter(r=>r.processLink?.enabled&&r.processLink?.process).length;return count?`${count} etapa(s) com processo associado`:"Nenhuma etapa vinculada a processo"}
function entregaMtCompatibility(){
 const marked=normalizeSubactions(state.subactions).filter(r=>r.entregaMt?.enabled);
 if(!marked.length)return true;
 return marked.every(r=>r.entregaMt.type&&r.entregaMt.product&&r.entregaMt.unit&&r.entregaMt.quantity);
}
function processCompatibility(){
 const marked=normalizeStages(state.stages).filter(r=>r.processLink?.enabled);
 if(!marked.length)return true;
 return marked.every(r=>r.processLink.process&&r.processLink.responsible&&r.processLink.risk&&r.processLink.control&&r.processLink.evidence);
}
function entregaMtValidationCard(){
 const marked=normalizeSubactions(state.subactions).filter(r=>r.entregaMt?.enabled);
 if(!marked.length)return validationState("Compatibilidade com o Sistema Entregas MT","Não definida","valid neutral");
 return validationSummary("Compatibilidade com o Sistema Entregas MT",entregaMtCompatibility(),"✓ Compatível");
}
function processValidationCard(){
 const marked=normalizeStages(state.stages).filter(r=>r.processLink?.enabled);
 if(!marked.length)return validationState("Compatibilidade processual","Não definida","valid neutral");
 return validationSummary("Compatibilidade processual",processCompatibility(),"✓ Compatível");
}
function subactionStepMarkup(){
 return `<div class="form-section"><h3>Subações / entregas anuais</h3><p>Use linguagem concreta: o que será entregue, para quem e onde. Vincule cada entrega a um ou mais componentes ativos da EAP para permitir a contabilização direta da programação.</p><div id="subaction-list" class="dynamic-list">${subactionRows()}</div><button class="secondary-button add-row" data-list="subactions" type="button">＋ Adicionar subação</button></div>`;
}
function subactionRows(){
 const rows=normalizeSubactions(state.subactions).length?normalizeSubactions(state.subactions):[{text:"",date:"",components:[]}];
 const components=getPolicyComponents();
 return rows.map((r,i)=>`<div class="dynamic-row subaction-row" data-index="${i}"><input class="row-text" value="${htmlSafe(r.text||"")}" placeholder="Ex.: Implantar laboratórios de aprendizagem"><input class="row-date" type="date" value="${htmlSafe(r.date||"")}"><button class="remove-row" type="button">×</button><div class="component-picker"><span>Componentes vinculados à entrega</span>${components.length?`<div class="component-options">${components.map((c,j)=>`<label><input class="row-component" type="checkbox" value="${htmlSafe(c)}" ${((r.components||[]).includes(c))?"checked":""}><b>${j+1}</b>${htmlSafe(c)}</label>`).join("")}</div>`:`<p>Nenhum componente ativo localizado para esta política PAOE.</p>`}<small>Selecione um ou mais componentes quando a entrega materializar mais de uma frente de programação.</small></div>${regionalizationBlock(r)}${entregaMtBlock(r)}</div>`).join("");
}
function regionalizationBlock(row){
 return `<div class="nested-program-block"><label class="toggle-line"><input class="row-regionalize" type="checkbox" ${row.regionalize?"checked":""}> Programar esta mesma subação em uma ou mais regiões?</label><div class="nested-details ${row.regionalize?"":"hidden"}"><p>Ao selecionar mais de uma região, o SPO real poderá gerar os registros regionalizados correspondentes no banco.</p><div class="region-mini-grid">${regions.map(r=>`<label><input class="row-region" type="checkbox" value="${r[0]}" ${(row.regions||[]).includes(r[0])?"checked":""}> <span>${r[0]}</span><small>${r[1]}</small><input class="row-region-target" data-region="${r[0]}" type="number" min="0" value="${htmlSafe(row.regionTargets?.[r[0]]||"")}" placeholder="Meta"></label>`).join("")}</div></div></div>`;
}
function entregaMtBlock(row){
 const e=row.entregaMt||{};
 return `<div class="nested-program-block entregas-mt"><label class="toggle-line"><input class="row-entrega-mt" type="checkbox" ${e.enabled?"checked":""}> Esta subação possui entrega elegível para o Sistema Entregas MT?</label><div class="nested-details ${e.enabled?"":"hidden"}"><div class="form-grid"><label class="field"><span>Classificação da entrega</span><select class="entrega-type"><option value="">Selecione</option><option ${e.type==="Obra"?"selected":""}>Obra</option><option ${e.type==="Equipamento"?"selected":""}>Equipamento</option><option ${e.type==="Serviço"?"selected":""}>Serviço</option><option ${e.type==="Formação"?"selected":""}>Formação</option><option ${e.type==="Sistema/tecnologia"?"selected":""}>Sistema/tecnologia</option></select></label><label class="field"><span>Produto da entrega</span><input class="entrega-product" value="${htmlSafe(e.product||"")}" placeholder="Produto conforme regra do Entregas MT"></label><label class="field"><span>Unidade de medida</span><input class="entrega-unit" value="${htmlSafe(e.unit||"")}" placeholder="Ex.: unidade, aluno, escola"></label><label class="field"><span>Quantidade</span><input class="entrega-quantity" type="number" min="0" value="${htmlSafe(e.quantity||"")}"></label><label class="field full"><span>Observações / regra específica</span><textarea class="entrega-notes" placeholder="Detalhe a regra ou observação específica do Sistema Entregas MT.">${htmlSafe(e.notes||"")}</textarea></label></div></div></div>`;
}
function dynamicStep(title,desc,id,list,placeholder,name,button){return `<div class="form-section"><h3>${title}</h3><p>${desc}</p><div id="${id}" class="dynamic-list">${dynamicRows(list,placeholder)}</div><button class="secondary-button add-row" data-list="${name}" type="button">＋ ${button}</button></div>`}
function dynamicRows(list,placeholder){const rows=list.length?list:[{text:"",date:""}];return rows.map((r,i)=>`<div class="dynamic-row" data-index="${i}"><input class="row-text" value="${r.text||""}" placeholder="${placeholder}"><input class="row-date" type="date" value="${r.date||""}"><button class="remove-row" type="button">×</button></div>`).join("")}
function collect(name,box){state[name]=[...box.querySelectorAll(".dynamic-row")].map(r=>({text:r.querySelector(".row-text").value,date:r.querySelector(".row-date").value}))}
function stageStepMarkup(){
 return `<div class="form-section"><h3>Etapas e marcos</h3><p>Organize atividades verificáveis e, quando fizer sentido, vincule a etapa aos processos organizacionais que viabilizam a execução.</p><div id="stage-list" class="dynamic-list">${stageRows()}</div><button class="secondary-button add-row" data-list="stages" type="button">＋ Adicionar etapa</button></div>`;
}
function stageRows(){
 const rows=normalizeStages(state.stages).length?normalizeStages(state.stages):[{text:"",date:"",processLink:{}}];
 return rows.map((r,i)=>{const p=r.processLink||{};return `<div class="dynamic-row stage-row" data-index="${i}"><input class="row-text" value="${htmlSafe(r.text||"")}" placeholder="Ex.: Elaborar projeto executivo"><input class="row-date" type="date" value="${htmlSafe(r.date||"")}"><button class="remove-row" type="button">×</button><div class="nested-program-block process-link-block"><label class="toggle-line"><input class="stage-process-enabled" type="checkbox" ${p.enabled?"checked":""}> Vincular esta etapa a processo organizacional/gerencial?</label><div class="nested-details ${p.enabled?"":"hidden"}"><div class="form-grid"><label class="field"><span>Tipo de processo</span><select class="stage-process-type">${processTypeOptions(p.type||"support")}</select></label><label class="field"><span>Processo associado</span><select class="stage-process-name">${processNameOptions(p.type||"support",p.process||"")}</select></label><label class="field"><span>Responsável pelo processo</span><input class="stage-process-responsible" value="${htmlSafe(p.responsible||"")}" placeholder="Unidade ou responsável"></label><label class="field"><span>Risco da etapa</span><input class="stage-process-risk" value="${htmlSafe(p.risk||"")}" placeholder="Evento que pode prejudicar a etapa"></label><label class="field"><span>Controle previsto</span><input class="stage-process-control" value="${htmlSafe(p.control||"")}" placeholder="Controle ou mitigação"></label><label class="field"><span>Evidência esperada</span><input class="stage-process-evidence" value="${htmlSafe(p.evidence||"")}" placeholder="Documento, registro ou medição"></label></div></div></div></div>`}).join("");
}
function processTypeOptions(selected){return [["support","Processos de suporte"],["managerial","Processos gerenciais"],["finalistic","Processos finalísticos"]].map(([value,label])=>`<option value="${value}" ${selected===value?"selected":""}>${label}</option>`).join("")}
function processNameOptions(type,selected){const items=(processInfo()[type]||processInfo().support).items;return `<option value="">Selecione</option>`+items.map(item=>`<option ${selected===item?"selected":""}>${item}</option>`).join("")}
function collectSubactions(box){state.subactions=[...box.querySelectorAll(".subaction-row")].map(r=>{const regionTargets={};r.querySelectorAll(".row-region-target").forEach(input=>{if(input.value)regionTargets[input.dataset.region]=input.value});return {text:r.querySelector(".row-text").value,date:r.querySelector(".row-date").value,components:[...r.querySelectorAll(".row-component:checked")].map(c=>c.value),regionalize:r.querySelector(".row-regionalize")?.checked||false,regions:[...r.querySelectorAll(".row-region:checked")].map(c=>c.value),regionTargets,entregaMt:{enabled:r.querySelector(".row-entrega-mt")?.checked||false,type:r.querySelector(".entrega-type")?.value||"",product:r.querySelector(".entrega-product")?.value||"",unit:r.querySelector(".entrega-unit")?.value||"",quantity:r.querySelector(".entrega-quantity")?.value||"",notes:r.querySelector(".entrega-notes")?.value||""}}})}
function collectStages(box){state.stages=[...box.querySelectorAll(".stage-row")].map(r=>({text:r.querySelector(".row-text").value,date:r.querySelector(".row-date").value,processLink:{enabled:r.querySelector(".stage-process-enabled")?.checked||false,type:r.querySelector(".stage-process-type")?.value||"support",process:r.querySelector(".stage-process-name")?.value||"",responsible:r.querySelector(".stage-process-responsible")?.value||"",risk:r.querySelector(".stage-process-risk")?.value||"",control:r.querySelector(".stage-process-control")?.value||"",evidence:r.querySelector(".stage-process-evidence")?.value||""}}))}
function bindStepEvents(){
 if(state.step===0){["adjunct","unit","subfunction"].forEach(id=>{$(`#${id}`).value=state[id];$(`#${id}`).onchange=e=>{state[id]=e.target.value;renderContext()}})}
 if(state.step===1){document.querySelectorAll("input[name='region-reference']").forEach(input=>input.onchange=()=>{state.regions=input.checked?[input.value]:[];renderContext();renderWizard()})}
 if(state.step===2){const box=$("#subaction-list"),sync=()=>{collectSubactions(box);renderContext();renderTree()};box.oninput=sync;box.onchange=sync;box.querySelectorAll(".row-regionalize,.row-entrega-mt").forEach(input=>input.onchange=()=>{sync();renderWizard()});box.querySelectorAll(".remove-row").forEach(b=>b.onclick=()=>{b.closest(".dynamic-row").remove();sync();renderWizard()});$(".add-row").onclick=()=>{sync();state.subactions.push({text:"",date:"",components:[],regionalize:false,regions:[],regionTargets:{},entregaMt:{enabled:false}});renderWizard()}}
 if(state.step===3){const box=$("#stage-list"),sync=()=>{collectStages(box);renderContext();renderTree()};box.oninput=sync;box.onchange=sync;box.querySelectorAll(".stage-process-enabled").forEach(input=>input.onchange=()=>{sync();renderWizard()});box.querySelectorAll(".stage-process-type").forEach(select=>select.onchange=()=>{sync();const row=select.closest(".stage-row");const name=row.querySelector(".stage-process-name");name.innerHTML=processNameOptions(select.value,"");collectStages(box)});box.querySelectorAll(".remove-row").forEach(b=>b.onclick=()=>{b.closest(".dynamic-row").remove();sync();renderWizard()});$(".add-row").onclick=()=>{sync();state.stages.push({text:"",date:"",processLink:{enabled:false,type:"support"}});renderWizard()}}
 if(state.step===4)$("#budget").oninput=e=>{state.budget=e.target.value;renderContext();renderTree()}
}
function toast(message){const t=$("#toast");t.textContent=message;t.classList.add("show");setTimeout(()=>t.classList.remove("show"),4300)}
const isEmbeddedMode=document.body.classList.contains("embedded-mode");
const spoScrollbarTargets="body,.eap-table-wrap,.admin-panel,.support-text-editor,.eap-map-flow,.journey-panel,.context-panel,.workspace,.journey-tree,.context-components-panel,.context-goals-panel,.key-card code";
function isSpoScrollable(el){if(!el)return false;if(el===document.body)return document.documentElement.scrollHeight>window.innerHeight+1||document.body.scrollHeight>window.innerHeight+1;return el.scrollHeight>el.clientHeight+1||el.scrollWidth>el.clientWidth+1}
function setSpoScrollbarVisible(el,visible){if(!el)return;el.classList.toggle("scrollbar-visible",!!visible)}
function bindSpoScrollbar(el){if(!el||el.dataset?.spoScrollbarBound==="1")return;if(el.dataset)el.dataset.spoScrollbarBound="1";let timer=null;const show=()=>{if(!isSpoScrollable(el))return;setSpoScrollbarVisible(el,true);clearTimeout(timer);timer=setTimeout(()=>setSpoScrollbarVisible(el,false),900)};const move=ev=>{if(!isSpoScrollable(el))return;const rect=el===document.body?{right:window.innerWidth,bottom:window.innerHeight}:el.getBoundingClientRect();setSpoScrollbarVisible(el,ev.clientX>=rect.right-28||ev.clientY>=rect.bottom-28)};const hide=()=>{clearTimeout(timer);setSpoScrollbarVisible(el,false)};(el===document.body?window:el).addEventListener("scroll",show,{passive:true});el.addEventListener("mousemove",move,{passive:true});el.addEventListener("mouseleave",hide,{passive:true})}
function refreshSpoScrollbars(){if(!isEmbeddedMode)return;document.querySelectorAll(spoScrollbarTargets).forEach(bindSpoScrollbar)}
function pta2027CurrentView(){const wizard=$("#wizard-view");return wizard&&!wizard.classList.contains("hidden")?"wizard":"value"}
function pta2027StorageStatus(){return ["spo-strategic-elements","spo-strategic-pending","spo-support-texts","spo-support-texts-pending","spo-eap-rows",pta2027DraftKey].reduce((acc,key)=>{acc[key]=Boolean(localStorage.getItem(key));return acc},{})}
function syncPta2027RuntimeMarkers(){document.body.dataset.pta2027Embedded=isEmbeddedMode?"1":"0";document.body.dataset.pta2027View=pta2027CurrentView();document.body.dataset.pta2027Programming=document.body.classList.contains("programming-focus")?"on":"off";document.body.dataset.pta2027Step=String(state.step??"");document.body.dataset.pta2027Macro=state.macro?.code||"";document.body.dataset.pta2027Axis=state.axis?.[0]||""}
function pta2027GetRuntimeStatus(){syncPta2027RuntimeMarkers();return {kind:"pta2027-v63r",embeddedMode:isEmbeddedMode,programmingFocus:document.body.classList.contains("programming-focus"),theme:document.body.classList.contains("dark")?"dark":"light",view:pta2027CurrentView(),state:{macroCode:state.macro?.code||null,macroName:state.macro?.name||null,axisName:state.axis?.[0]||null,axisPolicy:state.axis?.[2]||null,stepIndex:state.step,stepLabel:steps[state.step]||null},counts:{treeSteps:document.querySelectorAll(".tree-step").length,contextCards:document.querySelectorAll("#context-cards .context-item").length,collapsedContextPanels:document.querySelectorAll("[data-mobile-context-panel].is-collapsed").length,subactions:normalizeSubactions(state.subactions).length,stages:normalizeStages(state.stages).length,eapRows:eapRows.length},storage:pta2027StorageStatus(),bodyDataset:{...document.body.dataset}}}
window.pta2027GetRuntimeStatus=pta2027GetRuntimeStatus;
function postEmbeddedHeight(){if(!isEmbeddedMode||!window.parent||window.parent===window)return;const visible=document.querySelector("#wizard-view:not(.hidden),#value-view:not(.hidden)")||document.body;const appbar=document.querySelector(".appbar");const appbarHeight=appbar&&getComputedStyle(appbar).display!=="none"?Math.ceil(appbar.getBoundingClientRect().height):0;const height=Math.max(visible.scrollHeight+appbarHeight,Math.ceil(visible.getBoundingClientRect().height)+appbarHeight,720)+2;window.parent.postMessage({type:"pta2027-height",height},window.location.origin)}
function requestEmbeddedLayout(){syncPta2027RuntimeMarkers();if(!isEmbeddedMode)return;requestAnimationFrame(()=>requestAnimationFrame(()=>{refreshSpoScrollbars();postEmbeddedHeight()}))}
function requestImmersiveMode(enabled){if(!isEmbeddedMode||!window.parent||window.parent===window)return;window.parent.postMessage({type:"pta2027-immersive",enabled:!!enabled},window.location.origin)}
function requestContextFocus(enabled){document.body.classList.toggle("programming-focus",!!enabled);syncPta2027RuntimeMarkers();if(!isEmbeddedMode||!window.parent||window.parent===window)return;window.parent.postMessage({type:"pta2027-context-focus",enabled:!!enabled},window.location.origin);requestEmbeddedLayout()}
const mobileContextQuery=window.matchMedia?window.matchMedia("(max-width: 760px)"):null;
function updateMobileContextPanel(panel){const collapsed=panel.classList.contains("is-collapsed");const body=panel.querySelector(".mobile-context-body");const mobile=mobileContextQuery?.matches||false;if(mobile){panel.setAttribute("role","button");panel.setAttribute("tabindex","0");panel.setAttribute("aria-expanded",String(!collapsed));if(body?.id)panel.setAttribute("aria-controls",body.id);panel.dataset.mobileContextState=collapsed?"collapsed":"expanded";return}panel.removeAttribute("role");panel.removeAttribute("tabindex");panel.removeAttribute("aria-expanded");panel.removeAttribute("aria-controls");delete panel.dataset.mobileContextState}
function toggleMobileContextPanel(panel){if(!(mobileContextQuery?.matches))return;panel.dataset.mobileContextTouched="1";panel.classList.toggle("is-collapsed");updateMobileContextPanel(panel);requestEmbeddedLayout()}
function syncMobileContextPanels(){const mobile=mobileContextQuery?.matches||false;document.querySelectorAll("[data-mobile-context-panel]").forEach(panel=>{if(!mobile){panel.classList.remove("is-collapsed");panel.dataset.mobileContextReady="0";updateMobileContextPanel(panel);return}if(panel.dataset.mobileContextReady!=="1"&&!panel.dataset.mobileContextTouched){panel.classList.add("is-collapsed")}panel.dataset.mobileContextReady="1";updateMobileContextPanel(panel)});requestEmbeddedLayout()}
function setupMobileContextPanels(){document.querySelectorAll("[data-mobile-context-panel]").forEach(panel=>{panel.addEventListener("click",event=>{if(event.target.closest("a,button,input,select,textarea,label"))return;toggleMobileContextPanel(panel)});panel.addEventListener("keydown",event=>{if(event.key!=="Enter"&&event.key!==" ")return;event.preventDefault();toggleMobileContextPanel(panel)})});if(mobileContextQuery?.addEventListener)mobileContextQuery.addEventListener("change",syncMobileContextPanels);else if(mobileContextQuery?.addListener)mobileContextQuery.addListener(syncMobileContextPanels);syncMobileContextPanels()}
function applySpoHostUser(payload={}){if(!isEmbeddedMode)return;const meta=$("#pta-standalone-clock");const avatar=document.querySelector(".avatar");if(meta&&payload.userMeta)meta.textContent=payload.userMeta;if(avatar&&payload.userInitials)avatar.textContent=payload.userInitials}
function applySpoHostTheme(payload={}){if(!isEmbeddedMode)return;document.body.classList.toggle("dark",payload.theme==="dark");const root=document.documentElement;if(payload.accent)root.style.setProperty("--teal",payload.accent);if(payload.accentStrong)root.style.setProperty("--teal-dark",payload.accentStrong);if(payload.accentRgb)root.style.setProperty("--spo-accent-rgb",payload.accentRgb);if(payload.layoutPageGutter)root.style.setProperty("--spo-host-page-gutter",payload.layoutPageGutter);if(payload.layoutPanelGap)root.style.setProperty("--spo-host-panel-gap",payload.layoutPanelGap);if(payload.layoutSidebarCollapsed)root.style.setProperty("--spo-host-sidebar-collapsed",payload.layoutSidebarCollapsed);applySpoHostUser(payload);syncThemedSvgs(document);requestEmbeddedLayout()}
function returnToStrategicChain(){saveCurrentStepInputs();$("#wizard-view").classList.add("hidden");$("#value-view").classList.remove("hidden");requestImmersiveMode(true);requestContextFocus(false);requestEmbeddedLayout()}
function restoreDraftState(raw){
 let saved;
 try{saved=JSON.parse(raw)}catch(error){return {ok:false,message:"O rascunho local não pôde ser lido. Salve um novo rascunho para substituir este registro."}}
 if(!saved||typeof saved!=="object")return {ok:false,message:"O rascunho local está vazio ou incompleto."};
 const macroCode=saved.macro?.code||saved.macroCode;
 const macro=macros.find(m=>m.code===macroCode);
 if(!macro)return {ok:false,message:"O rascunho local não combina mais com o catálogo atual do PTA 2027."};
 const savedAxis=Array.isArray(saved.axis)?saved.axis:[];
 const axis=macro.axes.find(a=>a[1]===savedAxis[1]||a[2]===savedAxis[2]||a[0]===savedAxis[0]);
 if(!axis)return {ok:false,message:"O eixo salvo no rascunho não foi localizado no catálogo atual."};
 Object.assign(state,saved,{macro,axis});
 state.step=Math.min(Math.max(Number(saved.step)||0,0),steps.length-1);
 state.unit=saved.unit||"2";
 state.adjunct=saved.adjunct||"SAIP";
 state.subfunction=saved.subfunction||"361";
 state.regions=Array.isArray(saved.regions)?saved.regions.filter(Boolean):[];
 state.subactions=normalizeSubactions(saved.subactions);
 state.stages=normalizeStages(saved.stages);
 state.budget=saved.budget||"";
 state.contextGoalsOpen=!!saved.contextGoalsOpen;
 state.contextPolicyOpen=!!saved.contextPolicyOpen;
 state.contextOpenGoals=Array.isArray(saved.contextOpenGoals)?saved.contextOpenGoals:[];
 return {ok:true};
}
window.addEventListener("message",event=>{if(event.origin!==window.location.origin)return;if(event.data?.type==="spo-theme")applySpoHostTheme(event.data);if(event.data?.type==="spo-pta2027-back-to-map")returnToStrategicChain()});
if(isEmbeddedMode){refreshSpoScrollbars();window.addEventListener("load",requestEmbeddedLayout);window.addEventListener("resize",requestEmbeddedLayout);if("ResizeObserver"in window)new ResizeObserver(requestEmbeddedLayout).observe(document.body);document.addEventListener("click",()=>setTimeout(requestEmbeddedLayout,80),true);document.addEventListener("input",()=>setTimeout(requestEmbeddedLayout,80),true);document.addEventListener("change",()=>setTimeout(requestEmbeddedLayout,80),true)}
$("#next-step").onclick=()=>{saveCurrentStepInputs();if(state.step<5){state.step++;renderWizard();if(isEmbeddedMode)window.parent?.postMessage({type:"pta2027-scroll-top"},window.location.origin);else scrollTo({top:0,behavior:"smooth"});requestEmbeddedLayout()}else toast("Programação validada. Gabarito pronto para geração.")};
$("#previous-step").onclick=()=>{saveCurrentStepInputs();if(state.step>0){state.step--;renderWizard();requestEmbeddedLayout()}};
$("#save-draft").onclick=()=>{saveCurrentStepInputs();try{localStorage.setItem(pta2027DraftKey,JSON.stringify(state));toast("Rascunho salvo neste navegador.")}catch(error){toast("Não foi possível salvar o rascunho neste navegador.")}requestEmbeddedLayout()};
$("#back-to-map").onclick=returnToStrategicChain;
$("#theme-button").onclick=()=>{document.body.classList.toggle("dark");syncThemedSvgs(document);requestEmbeddedLayout()};
$("#resume-button").onclick=()=>{const raw=localStorage.getItem(pta2027DraftKey);if(!raw)return toast("Ainda não há rascunho salvo.");const restored=restoreDraftState(raw);if(!restored.ok)return toast(restored.message);$("#value-view").classList.add("hidden");$("#wizard-view").classList.remove("hidden");$("#journey-title").textContent=state.macro.name;requestImmersiveMode(true);requestContextFocus(true);renderWizard();requestEmbeddedLayout()};
function updateStandaloneClock(){const clock=$("#pta-standalone-clock");if(clock)clock.textContent=new Intl.DateTimeFormat("pt-BR",{dateStyle:"short",timeStyle:"medium"}).format(new Date())}
updateStandaloneClock();
setInterval(updateStandaloneClock,1000);
renderStrategicElements();renderPillars();renderMacros();setupEapTable();setupMobileContextPanels();
requestImmersiveMode(true);
requestContextFocus(false);
requestEmbeddedLayout();

