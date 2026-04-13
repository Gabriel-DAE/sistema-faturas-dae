import streamlit as st
import psycopg
from sqlalchemy import create_engine
import pandas as pd
import io
import pdfplumber
import re
from datetime import datetime
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

st.set_page_config(page_title="Gestão de Energia - DAE", layout="wide", page_icon="⚡")

# --- 1. SISTEMA DE LOGIN DAE ---
def check_password():
    """Valida se o usuário e senha digitados batem com os dados do Secrets."""
    def password_entered():
        # Verifica se o usuário existe no secrets e se a senha confere
        usuario_digitado = st.session_state["username"]
        senha_digitada = st.session_state["password"]
        
        if usuario_digitado in st.secrets["usuarios"] and senha_digitada == st.secrets["usuarios"][usuario_digitado]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Apaga a senha da memória por segurança
        else:
            st.session_state["password_correct"] = False

    # Se é a primeira vez abrindo o app, o status é Falso
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    # Se não estiver logado, desenha a tela de Login
    if not st.session_state["password_correct"]:
        # Criamos 3 colunas para centralizar a tela de login no meio do monitor
        col1, col2, col3 = st.columns([1, 1.5, 1])
        with col2:
            st.write("")
            st.write("")
            st.write("") # Espaçadores para empurrar pro meio da tela
            
            # Centraliza a logo usando HTML
            st.markdown(
                """
                <div style="text-align: center;">
                    <img src="https://via.placeholder.com/200x80?text=Logo+DAE" width="200">
                </div>
                """, unsafe_allow_html=True
            )
            # Dica: Substitua a linha acima pela sua imagem se tiver como subir o arquivo para o github.
            # Ex: st.image("logo_DAE.png", width=200)
            
            st.markdown("<h2 style='text-align: center; color: #0055A5;'>🔐 Acesso Restrito</h2>", unsafe_allow_html=True)
            st.markdown("<p style='text-align: center;'>Insira suas credenciais para acessar o Sistema de Faturas de Energia.</p>", unsafe_allow_html=True)
            
            st.text_input("👤 Usuário DAE", key="username")
            st.text_input("🔑 Senha", type="password", key="password")
            
            if st.button("🚀 Entrar no Sistema", type="primary", on_click=password_entered, use_container_width=True):
                pass
            
            if st.session_state.get("password_correct") == False:
                st.error("🚨 Usuário ou senha incorretos. Tente novamente.")
                
        return False # Interrompe aqui e não deixa ver o resto
    
    return True # Se a senha bateu, libera o acesso

# --- 2. A "CATRACA" DO STREAMLIT ---
# Se a função retornar falso (não logado), o st.stop() mata o código na hora e esconde o painel
if not check_password():
    st.stop()

# --- OCULTAR ELEMENTOS PADRÃO DO STREAMLIT ---
esconder_botoes = """
    <style>
    /* Oculta o menu hambúrguer no canto superior direito */
    #MainMenu {visibility: hidden;}
    
    /* Oculta o rodapé "Made with Streamlit" */
    footer {visibility: hidden;}
    
    /* Oculta o botão de "Deploy" se ele estiver aparecendo */
    .stAppDeployButton {display:none;}
    </style>
"""
st.markdown(esconder_botoes, unsafe_allow_html=True)

# --- CONEXÃO SEGURA COM O BANCO DE DADOS ---
# Se for rodar localmente antes de por na nuvem, crie uma pasta .streamlit e um arquivo secrets.toml
try:
    DATABASE_URL = st.secrets["DATABASE_URL"]
except FileNotFoundError:
    st.error("Erro: Arquivo secrets.toml não encontrado ou DATABASE_URL não configurada no Streamlit Cloud.")
    st.stop()

def obter_conexao():
    return psycopg.connect(DATABASE_URL, autocommit=True, prepare_threshold=None)

# --- CABEÇALHO COM LOGOTIPO ---
col_logo, col_titulo, _ = st.columns([0.6, 5, 1])

with col_logo:
    st.image("logo_DAE.png", use_container_width=True)

with col_titulo:
    st.subheader("⚡ Sistema de Faturas de Energia - DAE Bauru")
    st.markdown("*Sistema de Análise Técnica e Auditoria Tarifária*")

# --- 1. BANCO DE DADOS: CRIAÇÃO E PRÉ-CADASTRO ---
def inicializar_banco():
    conexao = obter_conexao()
    cursor = conexao.cursor()
    
    # 1. Cria a tabela principal de faturas (Mudança de AUTOINCREMENT para SERIAL no Postgres)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS faturas_cpfl (
            id SERIAL PRIMARY KEY,
            classificacao TEXT, unidade_consumidora TEXT, nome_unidade TEXT, atividade TEXT,
            periodo_leitura_inicio TEXT, periodo_leitura_fim TEXT, data_proxima_leitura TEXT,
            mes_referencia TEXT, data_vencimento TEXT,
            
            demanda_contratada_ponta REAL, demanda_contratada_fponta REAL,
            
            consumo_ponta REAL, tarifa_aneel_cons_ponta_tusd REAL, tarifa_trib_cons_ponta_tusd REAL, valor_cons_ponta_tusd REAL,
            consumo_fora_ponta REAL, tarifa_aneel_cons_fponta_tusd REAL, tarifa_trib_cons_fponta_tusd REAL, valor_cons_fponta_tusd REAL,
            
            tarifa_aneel_cons_ponta_te REAL, tarifa_trib_cons_ponta_te REAL, valor_cons_ponta_te REAL,
            tarifa_aneel_cons_fponta_te REAL, tarifa_trib_cons_fponta_te REAL, valor_cons_fponta_te REAL,
            
            tipo_bandeira TEXT, adicional_bandeira REAL,
            
            demanda_registrada_ponta REAL, tarifa_aneel_dem_ponta REAL, tarifa_trib_dem_ponta REAL, valor_dem_ponta REAL,
            demanda_isenta_ponta REAL, tarifa_aneel_dem_isenta_ponta REAL, tarifa_trib_dem_isenta_ponta REAL, valor_dem_isenta_ponta REAL,
            demanda_registrada_fora_ponta REAL, tarifa_aneel_dem_fponta REAL, tarifa_trib_dem_fponta REAL, valor_dem_fponta REAL,
            demanda_isenta_fora_ponta REAL, tarifa_aneel_dem_isenta_fponta REAL, tarifa_trib_dem_isenta_fponta REAL, valor_dem_isenta_fponta REAL,
            
            consumo_reativo_ponta REAL, tarifa_aneel_cons_reativo_ponta REAL, tarifa_trib_cons_reativo_ponta REAL, valor_cons_reativo_ponta REAL,
            consumo_reativo_fora_ponta REAL, tarifa_aneel_cons_reativo_fponta REAL, tarifa_trib_cons_reativo_fponta REAL, valor_cons_reativo_fponta REAL,
            
            demanda_ultrapassagem_ponta REAL, tarifa_aneel_dem_ultrap_ponta REAL, tarifa_trib_dem_ultrap_ponta REAL, valor_dem_ultrap_ponta REAL,
            demanda_ultrapassagem_fora_ponta REAL, tarifa_aneel_dem_ultrap_fponta REAL, tarifa_trib_dem_ultrap_fponta REAL, valor_dem_ultrap_fponta REAL,
            
            demanda_reativa_ponta REAL, tarifa_aneel_dem_reativa_ponta REAL, tarifa_trib_dem_reativa_ponta REAL, valor_dem_reativa_ponta REAL,
            demanda_reativa_fora_ponta REAL, tarifa_aneel_dem_reativa_fponta REAL, tarifa_trib_dem_reativa_fponta REAL, valor_dem_reativa_fponta REAL,
            
            cip REAL, retencao_consumo_irrf REAL, retencao_demanda_irrf REAL,
            valor_total_pis REAL, valor_total_cofins REAL, valor_total_icms REAL, valor_total_fatura REAL,
            data_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 2. Cria a tabela de Cadastro de UCs
    cursor.execute('''CREATE TABLE IF NOT EXISTS cadastro_uc (unidade_consumidora TEXT PRIMARY KEY, nome_unidade TEXT, atividade TEXT, classificacao TEXT, demanda_contratada_ponta REAL, demanda_contratada_fponta REAL)''')
    
    # 3. Cria a tabela de Tarifas
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS parametros_faturamento (
            mes_referencia TEXT,
            classificacao TEXT,
            aliq_pis REAL, aliq_cofins REAL, aliq_icms REAL,
            tar_aneel_cons_p_tusd REAL, tar_aneel_cons_fp_tusd REAL,
            tar_aneel_cons_p_te REAL, tar_aneel_cons_fp_te REAL,
            tar_aneel_dem_p REAL, tar_aneel_dem_fp REAL, tar_aneel_reativo REAL,
            tar_bandeira_vigente REAL, cip_padrao REAL, tipo_bandeira TEXT,
            PRIMARY KEY (mes_referencia, classificacao)
        )
    ''')
    
    # 4. Insere Tarifa Padrão inicial (Adaptado para sintaxe Postgres ON CONFLICT DO NOTHING)
    cursor.execute('''
        INSERT INTO parametros_faturamento VALUES (
            'DEZ/2025', 'Tarifa Azul-A4', 0.0107, 0.0492, 0.1800,
            0.11447000, 0.11447000, 0.44454000, 0.27119000, 48.61000000, 15.93000000, 0.28738000, 0.00000, 0.00, 'VERDE'
        ) ON CONFLICT (mes_referencia, classificacao) DO NOTHING;
    ''')
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_faturas_uc_mes ON faturas_cpfl (unidade_consumidora, mes_referencia);")
    
    conexao.commit()
    conexao.close()

inicializar_banco()

# --- 2. FUNÇÕES DE EXTRAÇÃO DE PDF ---
@st.cache_data(show_spinner="Carregando e processando banco de dados...")
def carregar_dados():
    # Usando SQLAlchemy para facilitar a vida do Pandas ao ler do Postgres
    url_sqlalchemy = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://")
    engine = create_engine(url_sqlalchemy)
    df = pd.read_sql_query("SELECT * FROM faturas_cpfl", engine)
    
    if df.empty:
        return df
        
    dicionario_nomes = {
        'classificacao': 'Classificação', 'unidade_consumidora': 'UC', 'nome_unidade': 'Nome da Unidade',
        'atividade': 'Atividade', 'periodo_leitura_inicio': 'Leitura Anterior', 'periodo_leitura_fim': 'Leitura Atual',
        'data_proxima_leitura': 'Próxima Leitura', 'mes_referencia': 'Mês Referência', 'data_vencimento': 'Vencimento',
        'demanda_contratada_ponta': 'Dem. Contr. Ponta', 'demanda_contratada_fponta': 'Dem. Contr. F.Ponta',
        'consumo_ponta': 'Consumo Ponta', 'tarifa_aneel_cons_ponta_tusd': 'Tarifa Cons. Ponta TUSD',
        'tarifa_trib_cons_ponta_tusd': 'Tarifa Trib. Cons. Ponta TUSD', 'valor_cons_ponta_tusd': 'Valor Cons. Ponta TUSD',
        'consumo_fora_ponta': 'Consumo F.Ponta', 'tarifa_aneel_cons_fponta_tusd': 'Tarifa Cons. F.Ponta TUSD',
        'tarifa_trib_cons_fponta_tusd': 'Tarifa Trib. Cons. F.Ponta TUSD', 'valor_cons_fponta_tusd': 'Valor Cons. F.Ponta TUSD',
        'tarifa_aneel_cons_ponta_te': 'Tarifa Cons. Ponta TE', 'tarifa_trib_cons_ponta_te': 'Tarifa Trib. Cons. Ponta TE',
        'valor_cons_ponta_te': 'Valor Cons. Ponta TE', 'tarifa_aneel_cons_fponta_te': 'Tarifa Cons. F.Ponta TE',
        'tarifa_trib_cons_fponta_te': 'Tarifa Trib. Cons. F.Ponta TE', 'valor_cons_fponta_te': 'Valor Cons. F.Ponta TE',
        'tipo_bandeira': 'Bandeira', 'adicional_bandeira': 'Adicional Bandeira', 'demanda_registrada_ponta': 'Dem. Reg. Ponta',
        'tarifa_aneel_dem_ponta': 'Tarifa Dem. Ponta', 'tarifa_trib_dem_ponta': 'Tarifa Trib. Dem. Ponta',
        'valor_dem_ponta': 'Valor Dem. Ponta', 'demanda_isenta_ponta': 'Dem. Isenta Ponta',
        'tarifa_aneel_dem_isenta_ponta': 'Tarifa Dem. Isenta Ponta', 'tarifa_trib_dem_isenta_ponta': 'Tarifa Trib. Dem. Isenta Ponta',
        'valor_dem_isenta_ponta': 'Valor Dem. Isenta Ponta', 'demanda_registrada_fora_ponta': 'Dem. Reg. F.Ponta',
        'tarifa_aneel_dem_fponta': 'Tarifa Dem. F.Ponta', 'tarifa_trib_dem_fponta': 'Tarifa Trib. Dem. F.Ponta',
        'valor_dem_fponta': 'Valor Dem. F.Ponta', 'demanda_isenta_fora_ponta': 'Dem. Isenta F.Ponta',
        'tarifa_aneel_dem_isenta_fponta': 'Tarifa Dem. Isenta F.Ponta', 'tarifa_trib_dem_isenta_fponta': 'Tarifa Trib. Dem. Isenta F.Ponta',
        'valor_dem_isenta_fponta': 'Valor Dem. Isenta F.Ponta', 'consumo_reativo_ponta': 'Cons. Reat. Ponta',
        'tarifa_aneel_cons_reativo_ponta': 'Tarifa Cons. Reat. Ponta', 'tarifa_trib_cons_reativo_ponta': 'Tarifa Trib. Cons. Reat. Ponta',
        'valor_cons_reativo_ponta': 'Valor Cons. Reat. Ponta', 'consumo_reativo_fora_ponta': 'Cons. Reat. F.Ponta',
        'tarifa_aneel_cons_reativo_fponta': 'Tarifa Cons. Reat. F.Ponta', 'tarifa_trib_cons_reativo_fponta': 'Tarifa Trib. Cons. Reat. F.Ponta',
        'valor_cons_reativo_fponta': 'Valor Cons. Reat. F.Ponta', 'demanda_ultrapassagem_ponta': 'Dem. Ultrap. Ponta',
        'tarifa_aneel_dem_ultrap_ponta': 'Tarifa Dem. Ultrap. Ponta', 'tarifa_trib_dem_ultrap_ponta': 'Tarifa Trib. Dem. Ultrap. Ponta',
        'valor_dem_ultrap_ponta': 'Valor Dem. Ultrap. Ponta', 'demanda_ultrapassagem_fora_ponta': 'Dem. Ultrap. F.Ponta',
        'tarifa_aneel_dem_ultrap_fponta': 'Tarifa Dem. Ultrap. F.Ponta', 'tarifa_trib_dem_ultrap_fponta': 'Tarifa Trib. Dem. Ultrap. F.Ponta',
        'valor_dem_ultrap_fponta': 'Valor Dem. Ultrap. F.Ponta', 'demanda_reativa_ponta': 'Dem. Reat. Ponta',
        'tarifa_aneel_dem_reativa_ponta': 'Tarifa Dem. Reat. Ponta', 'tarifa_trib_dem_reativa_ponta': 'Tarifa Trib. Dem. Reat. Ponta',
        'valor_dem_reativa_ponta': 'Valor Dem. Reat. Ponta', 'demanda_reativa_fora_ponta': 'Dem. Reat. F.Ponta',
        'tarifa_aneel_dem_reativa_fponta': 'Tarifa Dem. Reat. F.Ponta', 'tarifa_trib_dem_reativa_fponta': 'Tarifa Trib. Dem. Reat. F.Ponta',
        'valor_dem_reativa_fponta': 'Valor Dem. Reat. F.Ponta', 'cip': 'CIP', 'retencao_consumo_irrf': 'Retenção Cons. IRRF',
        'retencao_demanda_irrf': 'Retenção Dem. IRRF', 'valor_total_pis': 'Valor PIS', 'valor_total_cofins': 'Valor COFINS',
        'valor_total_icms': 'Valor ICMS', 'valor_total_fatura': 'Valor Total Fatura', 'data_insercao': 'Data Cadastro'
    }
    
    df = df.rename(columns=dicionario_nomes)
    
    df['Total Consumo'] = df['Consumo Ponta'] + df['Consumo F.Ponta']
    df['Valor Total Consumo'] = df['Valor Cons. Ponta TUSD'] + df['Valor Cons. Ponta TE'] + df['Valor Cons. F.Ponta TUSD'] + df['Valor Cons. F.Ponta TE']
    df['Valor Total Dem. Isenta'] = df['Valor Dem. Isenta Ponta'] + df['Valor Dem. Isenta F.Ponta']
    df['Valor Total Dem. Ultrap.'] = df['Valor Dem. Ultrap. Ponta'] + df['Valor Dem. Ultrap. F.Ponta']
    
    df['Valor Total Desv. Dem.'] = df['Valor Dem. Isenta F.Ponta']+df['Valor Dem. Isenta Ponta']+df['Valor Dem. Ultrap. F.Ponta']+df['Valor Dem. Ultrap. Ponta']
    
    df['Total Cons. Reat.'] = df['Cons. Reat. Ponta'] + df['Cons. Reat. F.Ponta']
    df['Valor Total Cons. Reat.'] = df['Valor Cons. Reat. Ponta'] + df['Valor Cons. Reat. F.Ponta']
    df['Valor Total Dem. Reat.'] = df['Valor Dem. Reat. Ponta'] + df['Valor Dem. Reat. F.Ponta']

    df['Valor Total Dem.'] = df['Valor Dem. Ponta'] + df['Valor Dem. F.Ponta']
    df['Valor Total Reativo'] = df['Valor Total Cons. Reat.'] + df['Valor Total Dem. Reat.']

    mes_map = {'JAN': '01', 'FEV': '02', 'MAR': '03', 'ABR': '04', 'MAI': '05', 'JUN': '06', 
               'JUL': '07', 'AGO': '08', 'SET': '09', 'OUT': '10', 'NOV': '11', 'DEZ': '12'}
    
    def converter_para_data_real(mes_str):
        try:
            m, y = str(mes_str).split('/')
            return pd.to_datetime(f"{y}-{mes_map[m.upper()]}-01")
        except:
            return pd.NaT

    df['Data Referência Oculta'] = df['Mês Referência'].apply(converter_para_data_real)
    df = df.sort_values(by=['Data Referência Oculta', 'UC'], ascending=[False, True])

    ordem_colunas = [
        'id', 'Data Referência Oculta', 'UC', 'Nome da Unidade', 'Atividade', 'Classificação', 'Mês Referência', 'Vencimento', 
        'Leitura Anterior', 'Leitura Atual', 'Próxima Leitura', 'Consumo Ponta', 'Tarifa Cons. Ponta TUSD', 'Tarifa Trib. Cons. Ponta TUSD', 'Valor Cons. Ponta TUSD', 
        'Tarifa Cons. Ponta TE', 'Tarifa Trib. Cons. Ponta TE', 'Valor Cons. Ponta TE', 'Consumo F.Ponta', 'Tarifa Cons. F.Ponta TUSD', 'Tarifa Trib. Cons. F.Ponta TUSD', 'Valor Cons. F.Ponta TUSD', 
        'Tarifa Cons. F.Ponta TE', 'Tarifa Trib. Cons. F.Ponta TE', 'Valor Cons. F.Ponta TE', 'Bandeira', 'Adicional Bandeira', 
        'Dem. Contr. Ponta', 'Dem. Reg. Ponta', 'Tarifa Dem. Ponta', 'Tarifa Trib. Dem. Ponta', 'Valor Dem. Ponta', 
        'Dem. Isenta Ponta', 'Tarifa Dem. Isenta Ponta', 'Tarifa Trib. Dem. Isenta Ponta', 'Valor Dem. Isenta Ponta', 
        'Dem. Ultrap. Ponta', 'Tarifa Dem. Ultrap. Ponta', 'Tarifa Trib. Dem. Ultrap. Ponta', 'Valor Dem. Ultrap. Ponta', 
        'Dem. Contr. F.Ponta', 'Dem. Reg. F.Ponta', 'Tarifa Dem. F.Ponta', 'Tarifa Trib. Dem. F.Ponta', 'Valor Dem. F.Ponta', 
        'Dem. Isenta F.Ponta', 'Tarifa Dem. Isenta F.Ponta', 'Tarifa Trib. Dem. Isenta F.Ponta', 'Valor Dem. Isenta F.Ponta', 
        'Dem. Ultrap. F.Ponta', 'Tarifa Dem. Ultrap. F.Ponta', 'Tarifa Trib. Dem. Ultrap. F.Ponta', 'Valor Dem. Ultrap. F.Ponta', 
        'Cons. Reat. Ponta', 'Tarifa Cons. Reat. Ponta', 'Tarifa Trib. Cons. Reat. Ponta', 'Valor Cons. Reat. Ponta', 
        'Cons. Reat. F.Ponta', 'Tarifa Cons. Reat. F.Ponta', 'Tarifa Trib. Cons. Reat. F.Ponta', 'Valor Cons. Reat. F.Ponta', 
        'Dem. Reat. Ponta', 'Tarifa Dem. Reat. Ponta', 'Tarifa Trib. Dem. Reat. Ponta', 'Valor Dem. Reat. Ponta', 
        'Dem. Reat. F.Ponta', 'Tarifa Dem. Reat. F.Ponta', 'Tarifa Trib. Dem. Reat. F.Ponta', 'Valor Dem. Reat. F.Ponta', 
        'CIP', 'Retenção Cons. IRRF', 'Retenção Dem. IRRF', 'Valor PIS', 'Valor COFINS', 'Valor ICMS', 
        'Total Consumo', 'Valor Total Consumo', 'Valor Total Dem.', 'Valor Total Dem. Isenta', 'Valor Total Dem. Ultrap.', 
        'Valor Total Desv. Dem.', 'Total Cons. Reat.', 'Valor Total Cons. Reat.', 'Valor Total Dem. Reat.', 'Valor Total Reativo',
        'Valor Total Fatura', 'Data Cadastro'
    ]
    
    colunas_finais = [c for c in ordem_colunas if c in df.columns]
    return df[colunas_finais]

def limpar_numero(texto_numero):
    if not texto_numero: return 0.0
    return float(texto_numero.replace('.', '').replace(',', '.'))

def extrair_texto_regex(padrao, texto, grupo=1, padrao_falha=""):
    res = re.search(padrao, texto, re.IGNORECASE | re.MULTILINE)
    return res.group(grupo).strip() if res else padrao_falha

def extrair_valor_regex(padrao, texto, grupo=1):
    res = re.search(padrao, texto, re.IGNORECASE | re.MULTILINE)
    return limpar_numero(res.group(grupo)) if res else 0.0

def processar_pdf(arquivo_pdf):
    with pdfplumber.open(arquivo_pdf) as pdf:
        texto = pdf.pages[0].extract_text()
        
    chaves_numericas = [
        'demanda_contratada_ponta', 'demanda_contratada_fponta',
        'consumo_ponta', 'tarifa_aneel_cons_ponta_tusd', 'tarifa_trib_cons_ponta_tusd', 'valor_cons_ponta_tusd',
        'consumo_fora_ponta', 'tarifa_aneel_cons_fponta_tusd', 'tarifa_trib_cons_fponta_tusd', 'valor_cons_fponta_tusd',
        'tarifa_aneel_cons_ponta_te', 'tarifa_trib_cons_ponta_te', 'valor_cons_ponta_te',
        'tarifa_aneel_cons_fponta_te', 'tarifa_trib_cons_fponta_te', 'valor_cons_fponta_te',
        'demanda_registrada_ponta', 'tarifa_aneel_dem_ponta', 'tarifa_trib_dem_ponta', 'valor_dem_ponta',
        'demanda_isenta_ponta', 'tarifa_aneel_dem_isenta_ponta', 'tarifa_trib_dem_isenta_ponta', 'valor_dem_isenta_ponta',
        'demanda_registrada_fora_ponta', 'tarifa_aneel_dem_fponta', 'tarifa_trib_dem_fponta', 'valor_dem_fponta',
        'demanda_isenta_fora_ponta', 'tarifa_aneel_dem_isenta_fponta', 'tarifa_trib_dem_isenta_fponta', 'valor_dem_isenta_fponta',
        'consumo_reativo_ponta', 'tarifa_aneel_cons_reativo_ponta', 'tarifa_trib_cons_reativo_ponta', 'valor_cons_reativo_ponta',
        'consumo_reativo_fora_ponta', 'tarifa_aneel_cons_reativo_fponta', 'tarifa_trib_cons_reativo_fponta', 'valor_cons_reativo_fponta',
        'demanda_ultrapassagem_ponta', 'tarifa_aneel_dem_ultrap_ponta', 'tarifa_trib_dem_ultrap_ponta', 'valor_dem_ultrap_ponta',
        'demanda_ultrapassagem_fora_ponta', 'tarifa_aneel_dem_ultrap_fponta', 'tarifa_trib_dem_ultrap_fponta', 'valor_dem_ultrap_fponta',
        'demanda_reativa_ponta', 'tarifa_aneel_dem_reativa_ponta', 'tarifa_trib_dem_reativa_ponta', 'valor_dem_reativa_ponta',
        'demanda_reativa_fora_ponta', 'tarifa_aneel_dem_reativa_fponta', 'tarifa_trib_dem_reativa_fponta', 'valor_dem_reativa_fponta'
    ]
    dados = {k: 0.0 for k in chaves_numericas}
    
    classificacao_bruta = extrair_texto_regex(r"Classificação:\s*(.*?)(?:\s+Serviço|\s+Tipo|\n)", texto)
    if "B3" in classificacao_bruta.upper():
        dados['classificacao'] = "Convencional B3"
    elif "VERDE" in classificacao_bruta.upper():
        dados['classificacao'] = "Tarifa Verde-A4"
    elif "AZUL" in classificacao_bruta.upper():
        dados['classificacao'] = "Tarifa Azul-A4"
    else:
        dados['classificacao'] = classificacao_bruta

    dados['unidade_consumidora'] = extrair_texto_regex(r"(\d{7,12})\s+\d{2}/\d{2}/\d{4}\s+\d{2}/\d{2}/\d{4}\s+\d{1,3}", texto)
    
    conexao_pdf = obter_conexao()
    c_pdf = conexao_pdf.cursor()
    # Usando o padrão Postgres %s em vez de ?
    c_pdf.execute("SELECT nome_unidade, atividade, demanda_contratada_ponta, demanda_contratada_fponta FROM cadastro_uc WHERE unidade_consumidora = %s", (dados['unidade_consumidora'],))
    res_uc = c_pdf.fetchone()
    conexao_pdf.close()
    
    dados['nome_unidade'] = res_uc[0] if res_uc else "Não Cadastrada"
    dados['atividade'] = res_uc[1] if res_uc else "Administrativa" 
    dados['demanda_contratada_ponta'] = res_uc[2] if res_uc else 0.0
    dados['demanda_contratada_fponta'] = res_uc[3] if res_uc else 0.0
    
    dc_p_pdf = extrair_valor_regex(r"Demanda P\.? kW\s+([\d\.,]+)", texto)
    if dc_p_pdf > 0: dados['demanda_contratada_ponta'] = dc_p_pdf
    
    dc_fp_pdf = extrair_valor_regex(r"Demanda FP\.? kW\s+([\d\.,]+)", texto)
    if dc_fp_pdf > 0: dados['demanda_contratada_fponta'] = dc_fp_pdf
    
    if "Verde" in dados['classificacao']:
        dc_unica = extrair_valor_regex(r"Demanda kW\s+([\d\.,]+)", texto)
        if dc_unica > 0: dados['demanda_contratada_fponta'] = dc_unica
        dados['demanda_contratada_ponta'] = 0.0
        
    if "B3" in dados['classificacao']:
        dados['demanda_contratada_ponta'] = 0.0
        dados['demanda_contratada_fponta'] = 0.0

    m_linha_principal = re.search(r"([A-Z]{3}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+R\$\s*([\d\.]+,\d{2})", texto)
    if m_linha_principal:
        dados['mes_referencia'] = m_linha_principal.group(1)
        dados['data_vencimento'] = m_linha_principal.group(2)
        dados['valor_total_fatura'] = limpar_numero(m_linha_principal.group(3))
    else:
        dados['mes_referencia'] = extrair_texto_regex(r"([A-Z]{3}/\d{4})\s+\d{2}/\d{2}/\d{4}\s+R\$", texto)
        dados['data_vencimento'] = extrair_texto_regex(r"[A-Z]{3}/\d{4}\s+(\d{2}/\d{2}/\d{4})\s+R\$", texto)
        dados['valor_total_fatura'] = 0.0

    dados['data_proxima_leitura'] = extrair_texto_regex(r"Próxima [Ll]eitura\s+(\d{2}/\d{2}/\d{4})", texto, padrao_falha="")
    
    leitura = re.search(r"\b\d{5,12}\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+\d{2,3}\b", texto)
    if leitura:
        dados['periodo_leitura_fim'] = leitura.group(1)
        dados['periodo_leitura_inicio'] = leitura.group(2)
    else:
        leitura_alt = re.search(r"(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+\d{2,3}", texto)
        dados['periodo_leitura_fim'] = leitura_alt.group(1) if leitura_alt else ""
        dados['periodo_leitura_inicio'] = leitura_alt.group(2) if leitura_alt else ""

    m_tusd_p = re.search(r"Consumo Ponta \[KWh\]\s*-\s*TUSD.*?kWh\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
    if m_tusd_p: dados['consumo_ponta'], dados['tarifa_aneel_cons_ponta_tusd'], dados['tarifa_trib_cons_ponta_tusd'], dados['valor_cons_ponta_tusd'] = [limpar_numero(x) for x in m_tusd_p.groups()]

    m_tusd_fp = re.search(r"Consumo Fora Ponta \[KWh\]\s*-\s*TUSD.*?kWh\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
    if m_tusd_fp: dados['consumo_fora_ponta'], dados['tarifa_aneel_cons_fponta_tusd'], dados['tarifa_trib_cons_fponta_tusd'], dados['valor_cons_fponta_tusd'] = [limpar_numero(x) for x in m_tusd_fp.groups()]

    m_te_p = re.search(r"Cons(?:umo)? Ponta\s*-\s*TE.*?kWh\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
    if m_te_p:
        vals = [limpar_numero(x) for x in m_te_p.groups()]
        dados['tarifa_aneel_cons_ponta_te'], dados['tarifa_trib_cons_ponta_te'], dados['valor_cons_ponta_te'] = vals[1], vals[2], vals[3]

    m_te_fp = re.search(r"Cons(?:umo)? F(?:ora)?\s*Ponta\s*TE.*?kWh\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
    if m_te_fp:
        vals = [limpar_numero(x) for x in m_te_fp.groups()]
        dados['tarifa_aneel_cons_fponta_te'], dados['tarifa_trib_cons_fponta_te'], dados['valor_cons_fponta_te'] = vals[1], vals[2], vals[3]

    if dados['consumo_fora_ponta'] == 0.0:
        m_tusd_b3 = re.search(r"Consumo Uso Sistema \[KWh\]\s*-\s*TUSD.*?kWh\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
        if m_tusd_b3: dados['consumo_fora_ponta'], dados['tarifa_aneel_cons_fponta_tusd'], dados['tarifa_trib_cons_fponta_tusd'], dados['valor_cons_fponta_tusd'] = [limpar_numero(x) for x in m_tusd_b3.groups()]

    if dados['valor_cons_fponta_te'] == 0.0:
        m_te_b3 = re.search(r"Consumo\s*-\s*TE.*?kWh\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
        if m_te_b3:
            vals = [limpar_numero(x) for x in m_te_b3.groups()]
            dados['tarifa_aneel_cons_fponta_te'], dados['tarifa_trib_cons_fponta_te'], dados['valor_cons_fponta_te'] = vals[1], vals[2], vals[3]

    dados['tipo_bandeira'] = extrair_texto_regex(r"Adicional Band (Verde|Amarela|Vermelha I|Vermelha II|Escassez Hídrica)", texto, padrao_falha="VERDE").upper()
    vp = extrair_valor_regex(r"Adicional Band.*?Ponta.*?kWh\s+([\d\.]+,\d{2})", texto)
    vfp = extrair_valor_regex(r"Adicional Band.*?FPonta.*?kWh\s+([\d\.]+,\d{2})", texto)
    dados['adicional_bandeira'] = vp + vfp

    linhas_ponta = re.findall(r"Demanda Ponta \[kW\]\s*-\s*TUSD.*?kW\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
    if len(linhas_ponta) >= 2:
        parsed = [[limpar_numero(x) for x in linha] for linha in linhas_ponta]
        parsed.sort(key=lambda x: x[2]) 
        dados['demanda_isenta_ponta'], dados['tarifa_aneel_dem_isenta_ponta'], dados['tarifa_trib_dem_isenta_ponta'], dados['valor_dem_isenta_ponta'] = parsed[0]
        dados['demanda_registrada_ponta'], dados['tarifa_aneel_dem_ponta'], dados['tarifa_trib_dem_ponta'], dados['valor_dem_ponta'] = parsed[1]
    elif len(linhas_ponta) == 1:
        dados['demanda_registrada_ponta'], dados['tarifa_aneel_dem_ponta'], dados['tarifa_trib_dem_ponta'], dados['valor_dem_ponta'] = [limpar_numero(x) for x in linhas_ponta[0]]

    linhas_fponta = re.findall(r"Demanda(?: F(?:ora)? Ponta)? \[kW\]\s*-\s*TUSD.*?kW\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
    if len(linhas_fponta) >= 2:
        parsed = [[limpar_numero(x) for x in linha] for linha in linhas_fponta]
        parsed.sort(key=lambda x: x[2])
        dados['demanda_isenta_fora_ponta'], dados['tarifa_aneel_dem_isenta_fponta'], dados['tarifa_trib_dem_isenta_fponta'], dados['valor_dem_isenta_fponta'] = parsed[0]
        dados['demanda_registrada_fora_ponta'], dados['tarifa_aneel_dem_fponta'], dados['tarifa_trib_dem_fponta'], dados['valor_dem_fponta'] = parsed[1]
    elif len(linhas_fponta) == 1:
        dados['demanda_registrada_fora_ponta'], dados['tarifa_aneel_dem_fponta'], dados['tarifa_trib_dem_fponta'], dados['valor_dem_fponta'] = [limpar_numero(x) for x in linhas_fponta[0]]

    m_reat_p = re.search(r"Consumo Reativo Exc Ponta.*?kWh\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
    if m_reat_p: dados['consumo_reativo_ponta'], dados['tarifa_aneel_cons_reativo_ponta'], dados['tarifa_trib_cons_reativo_ponta'], dados['valor_cons_reativo_ponta'] = [limpar_numero(x) for x in m_reat_p.groups()]

    m_reat_fp = re.search(r"Consumo Reativo Exc Fora Ponta.*?kWh\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
    if m_reat_fp: dados['consumo_reativo_fora_ponta'], dados['tarifa_aneel_cons_reativo_fponta'], dados['tarifa_trib_cons_reativo_fponta'], dados['valor_cons_reativo_fponta'] = [limpar_numero(x) for x in m_reat_fp.groups()]

    m_ultrap_p = re.search(r"Demanda Ultrap Ponta.*?kW\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
    if m_ultrap_p: dados['demanda_ultrapassagem_ponta'], dados['tarifa_aneel_dem_ultrap_ponta'], dados['tarifa_trib_dem_ultrap_ponta'], dados['valor_dem_ultrap_ponta'] = [limpar_numero(x) for x in m_ultrap_p.groups()]

    m_ultrap_fp = re.search(r"Demanda Ultrap(?:assagem)?(?: Fponta)?.*?kW\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
    if m_ultrap_fp: dados['demanda_ultrapassagem_fora_ponta'], dados['tarifa_aneel_dem_ultrap_fponta'], dados['tarifa_trib_dem_ultrap_fponta'], dados['valor_dem_ultrap_fponta'] = [limpar_numero(x) for x in m_ultrap_fp.groups()]

    m_dem_reat_p = re.search(r"Dem Reat Exc(?:ed)? Ponta.*?kW\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
    if m_dem_reat_p: dados['demanda_reativa_ponta'], dados['tarifa_aneel_dem_reativa_ponta'], dados['tarifa_trib_dem_reativa_ponta'], dados['valor_dem_reativa_ponta'] = [limpar_numero(x) for x in m_dem_reat_p.groups()]

    m_dem_reat_fp = re.search(r"Dem Reat Exc(?:ed)?(?: F(?:ora)? Ponta)?.*?kW\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)\s+([\d\.]+,\d+)", texto, re.IGNORECASE)
    if m_dem_reat_fp: dados['demanda_reativa_fora_ponta'], dados['tarifa_aneel_dem_reativa_fponta'], tarifa_trib_dem_reativa_fponta, dados['valor_dem_reativa_fponta'] = [limpar_numero(x) for x in m_dem_reat_fp.groups()]

    dados['cip'] = extrair_valor_regex(r"Contribuição Custeio IP-CIP.*?([\d\.]+,\d+)", texto)
    dados['retencao_consumo_irrf'] = extrair_valor_regex(r"Retencao Consumo IRRF-.*?([\d\.]+,[\d]{2})-", texto)
    dados['retencao_demanda_irrf'] = extrair_valor_regex(r"Retencao Demanda IRRF-.*?([\d\.]+,[\d]{2})-", texto)
    dados['valor_total_pis'] = extrair_valor_regex(r"PIS/PASEP.*?\s([\d\.]+,\d+)$", texto)
    dados['valor_total_cofins'] = extrair_valor_regex(r"COFINS.*?\s([\d\.]+,\d+)$", texto)
    dados['valor_total_icms'] = extrair_valor_regex(r"ICMS.*?\s([\d\.]+,\d+)$", texto)
    
    valor_pagar_fim = extrair_valor_regex(r"Total a Pagar\s+([\d\.]+,\d{2})", texto)
    if valor_pagar_fim > 0:
        dados['valor_total_fatura'] = valor_pagar_fim
        
    return dados

# --- 3. CÁLCULO INTELIGENTE ---
def calcular_faturamento(uc, mes, q_c_p, q_c_fp, q_d_reg_p, q_d_reg_fp, q_r_p, q_r_fp, q_dr_p, q_dr_fp, ret_cons_irrf, ret_dem_irrf):
    conexao = obter_conexao()
    cursor = conexao.cursor()
    
    cursor.execute("SELECT nome_unidade, atividade, classificacao, demanda_contratada_ponta, demanda_contratada_fponta FROM cadastro_uc WHERE unidade_consumidora = %s", (uc,))
    cadastro = cursor.fetchone()
    if not cadastro:
        return {"erro": f"Unidade Consumidora {uc} não encontrada. Vá na aba 'Configurações' e cadastre a UC e suas demandas."}
    
    nome_unidade, atividade, classificacao, dem_cont_p, dem_cont_fp = cadastro
    
    if "Verde" in classificacao:
        q_d_reg_p = 0.0 
        dem_cont_p = 0.0 
        
    if "B3" in classificacao:
        q_c_p = 0.0
        q_d_reg_p = 0.0
        q_d_reg_fp = 0.0
        dem_cont_p = 0.0
        dem_cont_fp = 0.0
        q_r_p = 0.0
        q_r_fp = 0.0
        q_dr_p = 0.0
        q_dr_fp = 0.0
    
    cursor.execute("SELECT * FROM parametros_faturamento WHERE mes_referencia = %s AND classificacao = %s", (mes, classificacao))
    params = cursor.fetchone()
    
    if not params:
        cursor.execute("SELECT * FROM parametros_faturamento WHERE classificacao = %s", (classificacao,))
        todos_params = cursor.fetchall()
        if todos_params:
            params = todos_params[-1]
            
    conexao.close()
    
    if not params:
        return {"erro": f"Nenhuma tarifa cadastrada para {classificacao} no sistema. Vá na aba de Configurações."}
    
    (_, _, pis, cofins, icms, t_c_p_tusd, t_c_fp_tusd, t_c_p_te, t_c_fp_te, t_d_p, t_d_fp, t_reat, t_bandeira, cip_padrao, tipo_bandeira) = params
    
    fator_pis_cofins = 1 - (pis + cofins)
    fator_icms = 1 - icms
    
    tot_icms = 0.0
    tot_pis = 0.0
    tot_cofins = 0.0
    subtotal_fatura = 0.0

    def calc_item(qtd, tarifa_aneel, tem_icms=True, multiplicador=1.0):
        nonlocal tot_icms, tot_pis, tot_cofins, subtotal_fatura
        if qtd <= 0: return tarifa_aneel, 0.0, 0.0
        
        tarifa_sem_icms = tarifa_aneel / fator_pis_cofins
        
        if tem_icms:
            tarifa_final = (tarifa_sem_icms / fator_icms) * multiplicador
            valor_total = qtd * tarifa_final
            v_icms = valor_total * icms
            v_p = (valor_total - v_icms) * pis
            v_c = (valor_total - v_icms) * cofins
        else:
            tarifa_final = tarifa_sem_icms * multiplicador
            valor_total = qtd * tarifa_final
            v_icms = 0.0
            v_p = valor_total * pis
            v_c = valor_total * cofins
            
        subtotal_fatura += valor_total
        tot_icms += v_icms
        tot_pis += v_p
        tot_cofins += v_c
        return tarifa_aneel, tarifa_final, valor_total

    ta_c_p_tusd, tt_c_p_tusd, v_c_p_tusd = calc_item(q_c_p, t_c_p_tusd, tem_icms=True)
    ta_c_fp_tusd, tt_c_fp_tusd, v_c_fp_tusd = calc_item(q_c_fp, t_c_fp_tusd, tem_icms=True)
    ta_c_p_te, tt_c_p_te, v_c_p_te = calc_item(q_c_p, t_c_p_te, tem_icms=True)
    ta_c_fp_te, tt_c_fp_te, v_c_fp_te = calc_item(q_c_fp, t_c_fp_te, tem_icms=True)
    
    adicional_bandeira = 0.0
    if t_bandeira > 0:
        _, _, adicional_bandeira = calc_item(q_c_p + q_c_fp, t_bandeira, tem_icms=True)

    ta_d_p, tt_d_p, v_d_reg_p = calc_item(q_d_reg_p, t_d_p, tem_icms=True)
    
    if q_d_reg_p < dem_cont_p:
        q_d_ise_p = dem_cont_p - q_d_reg_p
        _, tt_d_ise_p, v_d_ise_p = calc_item(q_d_ise_p, t_d_p, tem_icms=False)
    else:
        q_d_ise_p, tt_d_ise_p, v_d_ise_p = 0.0, 0.0, 0.0
        
    diferenca_p = q_d_reg_p - dem_cont_p
    if diferenca_p > (dem_cont_p * 0.05):
        q_up_p = diferenca_p
        _, tt_up_p, v_up_p = calc_item(q_up_p, t_d_p, tem_icms=True, multiplicador=2.0)
    else:
        q_up_p, tt_up_p, v_up_p = 0.0, 0.0, 0.0

    ta_d_fp, tt_d_fp, v_d_reg_fp = calc_item(q_d_reg_fp, t_d_fp, tem_icms=True)
    
    if q_d_reg_fp < dem_cont_fp:
        q_d_ise_fp = dem_cont_fp - q_d_reg_fp
        _, tt_d_ise_fp, v_d_ise_fp = calc_item(q_d_ise_fp, t_d_fp, tem_icms=False)
    else:
        q_d_ise_fp, tt_d_ise_fp, v_d_ise_fp = 0.0, 0.0, 0.0
        
    diferenca_fp = q_d_reg_fp - dem_cont_fp
    if diferenca_fp > (dem_cont_fp * 0.05):
        q_up_fp = diferenca_fp
        _, tt_up_fp, v_up_fp = calc_item(q_up_fp, t_d_fp, tem_icms=True, multiplicador=2.0)
    else:
        q_up_fp, tt_up_fp, v_up_fp = 0.0, 0.0, 0.0

    ta_r_p, tt_r_p, v_r_p = calc_item(q_r_p, t_reat, tem_icms=True)
    ta_r_fp, tt_r_fp, v_r_fp = calc_item(q_r_fp, t_reat, tem_icms=True)
    ta_dr_p, tt_dr_p, v_dr_p = calc_item(q_dr_p, t_d_fp, tem_icms=True) 
    ta_dr_fp, tt_dr_fp, v_dr_fp = calc_item(q_dr_fp, t_d_fp, tem_icms=True) 
    
    valor_total_fatura = subtotal_fatura + cip_padrao - ret_cons_irrf - ret_dem_irrf
    
    return {
        "nome_unidade": nome_unidade, "atividade": atividade, "demanda_contratada_ponta": dem_cont_p, "demanda_contratada_fponta": dem_cont_fp,
        "classificacao": classificacao,
        "ta_c_p_tusd": ta_c_p_tusd, "tt_c_p_tusd": tt_c_p_tusd, "v_c_p_tusd": v_c_p_tusd,
        "ta_c_fp_tusd": ta_c_fp_tusd, "tt_c_fp_tusd": tt_c_fp_tusd, "v_c_fp_tusd": v_c_fp_tusd,
        "ta_c_p_te": ta_c_p_te, "tt_c_p_te": tt_c_p_te, "v_c_p_te": v_c_p_te,
        "ta_c_fp_te": ta_c_fp_te, "tt_c_fp_te": tt_c_fp_te, "v_c_fp_te": v_c_fp_te,
        
        "q_d_ise_p": q_d_ise_p, "tt_d_ise_p": tt_d_ise_p, "v_d_ise_p": v_d_ise_p,
        "q_d_reg_p": q_d_reg_p, "ta_d_p": ta_d_p, "tt_d_p": tt_d_p, "v_d_reg_p": v_d_reg_p,
        "q_d_ise_fp": q_d_ise_fp, "tt_d_ise_fp": tt_d_ise_fp, "v_d_ise_fp": v_d_ise_fp,
        "q_d_reg_fp": q_d_reg_fp, "ta_d_fp": ta_d_fp, "tt_d_fp": tt_d_fp, "v_d_reg_fp": v_d_reg_fp,
        
        "q_up_p": q_up_p, "ta_up_p": ta_d_p, "tt_up_p": tt_up_p, "v_up_p": v_up_p,
        "q_up_fp": q_up_fp, "ta_up_fp": ta_d_fp, "tt_up_fp": tt_up_fp, "v_up_fp": v_up_fp,
        
        "ta_r_p": ta_r_p, "tt_r_p": tt_r_p, "v_r_p": v_r_p,
        "ta_r_fp": ta_r_fp, "tt_r_fp": tt_r_fp, "v_r_fp": v_r_fp,
        "ta_dr_p": ta_dr_p, "tt_dr_p": tt_dr_p, "v_dr_p": v_dr_p,
        "ta_dr_fp": ta_dr_fp, "tt_dr_fp": tt_dr_fp, "v_dr_fp": v_dr_fp,
        
        "tipo_bandeira": tipo_bandeira, "adicional_bandeira": adicional_bandeira, "cip_padrao": cip_padrao,
        "v_pis": tot_pis, "v_cofins": tot_cofins, "v_icms": tot_icms, "valor_total_fatura": valor_total_fatura
    }

# --- 4. INTERFACE ---
aba_dash, aba_dados, aba_pdf, aba_manual, aba_config = st.tabs(["📈 Dashboard", "📊 Banco de Dados", "📄 Upload PDF", "✍️ Cadastro Manual", "⚙️ Configurações"])

# ==========================================
# ABA DASHBOARD
# ==========================================
with aba_dash:
    df_dash = carregar_dados()
    
    if not df_dash.empty:
        st.markdown("##### ⚡ Business Intelligence - Consumo DAE")
        st.markdown("💡 **Dica PRO:** Segure a tecla **SHIFT** para seleção múltipla nos gráficos. Use os filtros superiores para seleções rápidas.")
        
        # 1. Preparação dos Dados
        df_dash['Ano'] = df_dash['Data Referência Oculta'].dt.year.astype(str)
        df_dash['Mes_Nome'] = df_dash['Data Referência Oculta'].dt.strftime('%B')
        df_dash['Mes_Num'] = df_dash['Data Referência Oculta'].dt.month
        
        # 2. Inicialização da Memória de Cliques
        if 'clique_ano' not in st.session_state: st.session_state.clique_ano = []
        if 'clique_mes' not in st.session_state: st.session_state.clique_mes = []
        if 'clique_uc' not in st.session_state: st.session_state.clique_uc = []

        st.divider()

        # --- SELETORES SUPERIORES E BOTÃO DE RESET ---
        dic_parametros = {
            "Consumo Total (kWh)": "Total Consumo",
            "Valor Total Fatura (R$)": "Valor Total Fatura",
            "Valor Total Consumo (R$)": "Valor Total Consumo",
            "Valor Total Demanda (R$)": "Valor Total Dem.",
            "Valor Total Desv. Dem. (R$)": "Valor Total Desv. Dem.",
            "Valor Total Reativo (R$)": "Valor Total Reativo"
        }
        
        # Ajuste de layout: Indicador (2.5), Classificação (2.5), Espaço (2), Botão (2)
        col_ind, col_cla, col_vazio, col_btn = st.columns([2.5, 2.5, 2, 2]) 
        
        param_nome = col_ind.selectbox("🎯 **Indicador:**", list(dic_parametros.keys()))
        param_coluna = dic_parametros[param_nome]
        is_dinheiro = "(R$)" in param_nome

        # Ajuste: Mudança para multiselect
        opcoes_classes = sorted(list(df_dash['Classificação'].unique()))
        filtro_classe_fixo = col_cla.multiselect("🏷️ **Classificação:**", options=opcoes_classes, placeholder="Todas as classes")

        # Botão de Reset (Extrema direita)
        if st.session_state.clique_ano or st.session_state.clique_mes or st.session_state.clique_uc:
            col_btn.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
            if col_btn.button("🧹 Limpar Filtros", use_container_width=True):
                st.session_state.clique_ano = []; st.session_state.clique_mes = []; st.session_state.clique_uc = []
                st.rerun()

        # 3. Lógica de Filtragem (Ajustada para Múltiplas Classes)
        df_filtrado_dash = df_dash.copy()
        
        # Se a lista de filtro não estiver vazia, aplica o filtro usando .isin()
        if filtro_classe_fixo:
            df_filtrado_dash = df_filtrado_dash[df_filtrado_dash['Classificação'].isin(filtro_classe_fixo)]
            
        # Aplica os cliques múltiplos (SHIFT)
        if st.session_state.clique_ano:
            df_filtrado_dash = df_filtrado_dash[df_filtrado_dash['Ano'].isin(st.session_state.clique_ano)]
        if st.session_state.clique_mes:
            df_filtrado_dash = df_filtrado_dash[df_filtrado_dash['Mes_Nome'].isin(st.session_state.clique_mes)]
        if st.session_state.clique_uc:
            df_filtrado_dash = df_filtrado_dash[df_filtrado_dash['Nome da Unidade'].isin(st.session_state.clique_uc)]

        st.write("")
        col_graf1, col_graf2 = st.columns(2)
        
        # --- GRÁFICO 1: Análise Anual ---
        df_ano = df_filtrado_dash.groupby('Ano')[param_coluna].sum().reset_index()
        fig_ano = px.bar(df_ano, x='Ano', y=param_coluna, text_auto='.3s', 
                         title=f"{param_nome} por Ano", color_discrete_sequence=["#0055A5"])
        fig_ano.update_layout(xaxis_title=None, yaxis_title=param_nome, xaxis={'type': 'category'})
        
        if is_dinheiro:
            fig_ano.update_traces(texttemplate='R$ %{y:.3s}', textposition='outside')
        else:
            fig_ano.update_traces(texttemplate='%{y:.3s}', textposition='outside')

        evento_ano = col_graf1.plotly_chart(fig_ano, use_container_width=True, on_select="rerun", selection_mode=("points", "box", "lasso"))
        
        if evento_ano and len(evento_ano.selection.get("points", [])) > 0:
            anos_sel = list(set([str(pt["x"]) for pt in evento_ano.selection["points"]]))
            if st.session_state.clique_ano != anos_sel:
                st.session_state.clique_ano = anos_sel; st.rerun()

        # --- GRÁFICO 2: Sazonalidade Mensal ---
        df_mes_ciclo = df_filtrado_dash.groupby(['Mes_Num', 'Mes_Nome'])[param_coluna].sum().reset_index().sort_values('Mes_Num')
        fig_mes = px.line(df_mes_ciclo, x='Mes_Nome', y=param_coluna, markers=True,
                          title=f"Sazonalidade Mensal ({param_nome})", color_discrete_sequence=["#0055A5"])
        fig_mes.update_layout(xaxis_title=None, yaxis_title=param_nome)
        fig_mes.update_yaxes(rangemode="tozero")
        
        if not df_mes_ciclo.empty:
            media_val = df_mes_ciclo[param_coluna].mean()
            fmt = f"Média: R$ {media_val:,.2f}" if is_dinheiro else f"Média: {media_val:,.0f}"
            fmt = fmt.replace(',', 'X').replace('.', ',').replace('X', '.')
            fig_mes.add_hline(y=media_val, line_dash="dash", line_color="#FF4B4B", annotation_text=fmt, annotation_position="top right")

        evento_mes = col_graf2.plotly_chart(fig_mes, use_container_width=True, on_select="rerun", selection_mode=("points", "box", "lasso"))
        
        if evento_mes and len(evento_mes.selection.get("points", [])) > 0:
            meses_sel = [str(pt["x"]) for pt in evento_mes.selection["points"]]
            if st.session_state.clique_mes != meses_sel:
                st.session_state.clique_mes = meses_sel; st.rerun()

        st.divider()
        
        # --- GRÁFICO 3: Top 20 Unidades ---
        st.markdown(f"#### 🏆 Top 20 Unidades por {param_nome}")
        df_top20 = df_filtrado_dash.groupby('Nome da Unidade')[param_coluna].sum().reset_index().sort_values(param_coluna, ascending=False).head(20)
        fig_top20 = px.bar(df_top20, x=param_coluna, y='Nome da Unidade', orientation='h', text_auto='.3s', color_discrete_sequence=["#0055A5"])
        fig_top20.update_layout(yaxis={'categoryorder':'total ascending'}, xaxis_title=param_nome, yaxis_title=None)
        
        if is_dinheiro:
            fig_top20.update_traces(texttemplate='R$ %{x:.3s}', textposition='outside')
        else:
            fig_top20.update_traces(texttemplate='%{x:.3s}', textposition='outside')
        
        evento_uc = st.plotly_chart(fig_top20, use_container_width=True, on_select="rerun", selection_mode=("points", "box", "lasso"))
        
        if evento_uc and len(evento_uc.selection.get("points", [])) > 0:
            ucs_sel = list(set([str(pt["y"]) for pt in evento_uc.selection["points"]]))
            if st.session_state.clique_uc != ucs_sel:
                st.session_state.clique_uc = ucs_sel; st.rerun()

# ==========================================
# ABA DADOS
# ==========================================
with aba_dados:
    df = carregar_dados()
    
    if not df.empty:
        st.markdown("##### 📋 Histórico Geral")
        st.markdown("💡 **Dica:** Marque as caixinhas no início das linhas para excluir múltiplos registros de uma só vez.")

        for col in df.columns:
            # Remove apenas o fuso horário das datas. O resto dos dados fica intacto!
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)

        # --- NOVO PAINEL DE FILTROS ---
        st.write("") # Dá um espacinho
        
        # Dividimos em 4 colunas agora
        col_f1, col_f2, col_f3, col_f4 = st.columns(4)
        
        # Cria as caixas de seleção buscando os dados únicos
        filtro_mes = col_f1.multiselect("📅 Filtrar por Mês", options=df['Mês Referência'].unique())
        filtro_classe = col_f2.multiselect("⚡ Filtrar Classificação", options=df['Classificação'].unique())
        filtro_uc = col_f3.multiselect("📍 Filtrar por UC", options=df['UC'].unique())
        filtro_busca = col_f4.text_input("🔍 Busca Livre", placeholder="Nome da unidade...")

        # Aplica a "peneira" sequencial nos dados
        df_filtrado = df.copy()
        
        if filtro_mes:
            df_filtrado = df_filtrado[df_filtrado['Mês Referência'].isin(filtro_mes)]
        if filtro_classe:
            df_filtrado = df_filtrado[df_filtrado['Classificação'].isin(filtro_classe)]
        if filtro_uc:
            df_filtrado = df_filtrado[df_filtrado['UC'].isin(filtro_uc)]
        if filtro_busca:
            df_filtrado = df_filtrado[
                df_filtrado['UC'].astype(str).str.contains(filtro_busca, case=False, na=False) |
                df_filtrado['Nome da Unidade'].astype(str).str.contains(filtro_busca, case=False, na=False)
            ]

        # --- TABELA NATIVA (AGORA COM DADOS FILTRADOS) ---
        evento = st.dataframe(
            df_filtrado, # Usa a tabela já filtrada
            hide_index=True,
            use_container_width=True,
            height=400,
            column_config={
                "id": None, 
                "Data Referência Oculta": None,
                "Valor Total Cons. Reat.": None,
                "Valor Total Dem. Reat.": None
            },
            selection_mode="multi-row",
            on_select="rerun"
        )
        
        linhas_selecionadas = evento.selection.rows
        
        if len(linhas_selecionadas) > 0:
            # Puxa o ID correto da tabela filtrada
            ids_para_excluir = [int(df_filtrado.iloc[i]['id']) for i in linhas_selecionadas]
            qtd_selecionada = len(ids_para_excluir)
            
            st.markdown(f"🔴 **{qtd_selecionada} Fatura(s) Selecionada(s)** para exclusão.")
            
            if st.session_state.get('confirmar_exclusao_ids') != ids_para_excluir:
                if st.button("🗑️ Excluir Selecionadas"):
                    st.session_state['confirmar_exclusao_ids'] = ids_para_excluir
                    st.rerun()
            else:
                st.warning(f"⚠️ TEM CERTEZA? Isso apagará permanentemente {qtd_selecionada} fatura(s) do banco de dados!")
                col1, col2, _ = st.columns([1, 1, 3]) 
                
                with col1:
                    if st.button("✅ Sim, apagar agora!", type="primary"):
                        conexao = obter_conexao()
                        c = conexao.cursor()
                        
                        # Preparação SQL para o Postgres (%s)
                        placeholders = ','.join('%s' for _ in ids_para_excluir)
                        query_exclusao = f"DELETE FROM faturas_cpfl WHERE id IN ({placeholders})"
                        
                        c.execute(query_exclusao, tuple(ids_para_excluir))
                        conexao.commit()
                        conexao.close()
                        
                        carregar_dados.clear()
                        
                        st.session_state['confirmar_exclusao_ids'] = None
                        st.success(f"{qtd_selecionada} fatura(s) excluída(s) com sucesso!")
                        st.rerun()
                with col2:
                    if st.button("❌ Cancelar"):
                        st.session_state['confirmar_exclusao_ids'] = None
                        st.rerun()

        st.divider()
        @st.cache_data(show_spinner=False)
        def gerar_excel_cache(df_para_excel):
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_para_excel.to_excel(writer, index=False, sheet_name='Historico_CPFL')
            return buffer.getvalue()

        df_limpo = df.drop(columns=['id', 'Data Referência Oculta'], errors='ignore')
        arquivo_excel_pronto = gerar_excel_cache(df_limpo)
        
        st.download_button(
            label="📥 Exportar Histórico Completo para Excel", 
            data=arquivo_excel_pronto, 
            file_name="Historico_Faturas_Geral.xlsx", 
            type="primary"
        )
    else:
        st.info("Nenhum dado encontrado.")

# ==========================================
# ABA IMPORTAÇÃO (PDF E EXCEL)
# ==========================================
with aba_pdf:
    st.markdown("##### 📥 Importação de Faturas")
    
    tab_pdf, tab_excel = st.tabs(["📄 Importação de Faturas (PDF)", "📊 Upload de Excel (Lote Histórico)"])
    
    with tab_pdf:
        st.markdown("Faça o upload dos arquivos PDF originais da CPFL para extração automática.")
        arquivos_upload = st.file_uploader("Selecione as faturas em PDF", type=["pdf"], accept_multiple_files=True)
        
        if arquivos_upload:
            if st.button("🚀 Extrair e Salvar Dados", type="primary"):
                sucessos = 0
                duplicadas = 0
                erros = 0
                
                conexao = obter_conexao()
                c = conexao.cursor()
                
                barra_progresso = st.progress(0)
                total_arquivos = len(arquivos_upload)
                
                for i, arquivo in enumerate(arquivos_upload):
                    try:
                        d = processar_pdf(arquivo)
                        
                        c.execute("SELECT id FROM faturas_cpfl WHERE unidade_consumidora = %s AND mes_referencia = %s", (d['unidade_consumidora'], d['mes_referencia']))
                        if c.fetchone():
                            duplicadas += 1
                        else:
                            colunas = ', '.join(d.keys())
                            # Postgres usa %s
                            placeholders = ', '.join(['%s'] * len(d))
                            valores = tuple(d.values())
                            c.execute(f"INSERT INTO faturas_cpfl ({colunas}) VALUES ({placeholders})", valores)
                            sucessos += 1
                            
                    except Exception as e:
                        erros += 1
                        st.error(f"Erro ao processar o arquivo '{arquivo.name}': {e}")
                    
                    barra_progresso.progress((i + 1) / total_arquivos)

                conexao.commit()
                carregar_dados.clear()
                conexao.close()
                
                st.markdown("### 📊 Relatório de Processamento")
                if sucessos > 0:
                    st.success(f"✅ **{sucessos}** faturas extraídas e salvas com sucesso!")
                    st.balloons()
                if duplicadas > 0:
                    st.warning(f"⚠️ **{duplicadas}** faturas foram ignoradas pois já existiam no banco de dados.")
                if erros > 0:
                    st.error(f"❌ **{erros}** faturas apresentaram erro durante a leitura.")

    with tab_excel:
        st.markdown("Faça o upload de uma planilha contendo o histórico de faturas antigas.")
        st.info("💡 **Dica:** O Excel deve conter os nomes exatos das colunas do banco de dados (em letras minúsculas).")
        
        arquivo_excel = st.file_uploader("Selecione a planilha Excel (.xlsx)", type=["xlsx"])
        
        if arquivo_excel is not None:
            if st.button("🚀 Processar e Salvar Faturas", type="primary"):
                try:
                    df_excel = pd.read_excel(arquivo_excel)
                    conexao = obter_conexao()
                    c = conexao.cursor()
                    
                    def formatar_data(valor):
                        if pd.isna(valor) or valor == '' or valor is None: return ""
                        if isinstance(valor, (int, float)):
                            if valor == 0: return ""
                            return (pd.to_datetime('1899-12-30') + pd.to_timedelta(valor, 'D')).strftime('%d/%m/%Y')
                        if hasattr(valor, 'strftime'):
                            return valor.strftime('%d/%m/%Y')
                        return str(valor).strip()

                    inseridas = 0
                    duplicadas = 0
                    
                    # Checando colunas no Postgres
                    c.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'faturas_cpfl'")
                    colunas_banco = [col[0] for col in c.fetchall() if col[0] not in ('id', 'data_insercao')]
                    
                    colunas_texto = ['classificacao', 'unidade_consumidora', 'nome_unidade', 'atividade', 
                                     'periodo_leitura_inicio', 'periodo_leitura_fim', 'data_proxima_leitura', 
                                     'mes_referencia', 'data_vencimento', 'tipo_bandeira']

                    for index, row in df_excel.iterrows():
                        uc = str(row.get('unidade_consumidora', '')).strip()
                        mes = str(row.get('mes_referencia', '')).strip()
                        
                        if not uc or not mes or uc == 'nan' or mes == 'nan':
                            continue
                        
                        c.execute("SELECT id FROM faturas_cpfl WHERE unidade_consumidora = %s AND mes_referencia = %s", (uc, mes))
                        if c.fetchone():
                            duplicadas += 1
                            continue
                        
                        dados_inserir = {}
                        for col in colunas_banco:
                            valor = row.get(col, None)
                            
                            if col in colunas_texto:
                                if pd.isna(valor) or valor == '' or valor is None or valor == 0.0:
                                    dados_inserir[col] = ""
                                elif col in ['data_vencimento', 'periodo_leitura_inicio', 'periodo_leitura_fim', 'data_proxima_leitura']:
                                    dados_inserir[col] = formatar_data(valor)
                                else:
                                    dados_inserir[col] = str(valor).strip()
                            else:
                                if pd.isna(valor) or valor == '' or valor is None:
                                    dados_inserir[col] = 0.0
                                else:
                                    try:
                                        dados_inserir[col] = float(valor)
                                    except:
                                        dados_inserir[col] = 0.0
                            
                        col_str = ', '.join(dados_inserir.keys())
                        placeholders = ', '.join(['%s'] * len(dados_inserir))
                        valores = tuple(dados_inserir.values())
                        
                        c.execute(f"INSERT INTO faturas_cpfl ({col_str}) VALUES ({placeholders})", valores)
                        inseridas += 1
                        
                    conexao.commit()
                    carregar_dados.clear()
                    conexao.close()
                    
                    st.success(f"✅ Processamento concluído! **{inseridas}** faturas inseridas com sucesso. **{duplicadas}** faturas ignoradas (já existiam no banco).")
                    if inseridas > 0:
                        st.balloons()
                        
                except Exception as e:
                    st.error(f"Erro ao processar a planilha. Verifique se os nomes das colunas estão corretos. Erro detalhado: {e}")

# ==========================================
# ABA INSERÇÃO MANUAL INTELIGENTE
# ==========================================
with aba_manual:
    st.markdown("##### 📝 Cadastro Manual de Fatura")
    st.markdown("Cadastre manualmente apenas as faturas que não possuírem arquivo PDF.")
    
    uc_input = st.text_input("🔍 1º Passo: Digite a Unidade Consumidora e aperte Enter", placeholder="Ex: 40190245").strip()
    
    classificacao_uc = "Tarifa Azul-A4"
    nome_uc = "Não Cadastrada"
    ativ_uc = "Administrativa"
    is_verde = False
    is_b3 = False
    
    if uc_input:
        conexao = obter_conexao()
        c = conexao.cursor()
        c.execute("SELECT nome_unidade, atividade, classificacao FROM cadastro_uc WHERE unidade_consumidora = %s", (uc_input,))
        res = c.fetchone()
        conexao.close()
        
        if res:
            nome_uc, ativ_uc, classificacao_uc = res
            st.success(f"📍 UC Localizada! Unidade: **{nome_uc}** | Atividade: **{ativ_uc}** | Classificação: **{classificacao_uc}**")
        else:
            st.warning("⚠️ UC não cadastrada nas configurações. O sistema usará o layout padrão (Azul-A4).")
            
    is_verde = ("Verde" in classificacao_uc)
    is_b3 = ("B3" in classificacao_uc)
    
    if 'form_key' not in st.session_state:
        st.session_state.form_key = 0
        
    with st.form(f"form_smart_{st.session_state.form_key}"):
        st.markdown("###### 2º Passo: Preencha os dados da fatura")
        c1, c2, c3 = st.columns(3)
        uc = c1.text_input("Unidade Consumidora", value=uc_input, disabled=True)
        mes = c2.text_input("Mês Referência", value="", placeholder="Ex: DEZ/2025", help="Obrigatório: 3 letras, barra, 4 números (Ex: JAN/2026)").strip().upper()
        vencimento_val = c3.date_input("Data Vencimento", value=None, format="DD/MM/YYYY")
        
        c4, c5, c6 = st.columns(3)
        p_fim_val = c4.date_input("Leitura Atual", value=None, format="DD/MM/YYYY")
        p_inicio_val = c5.date_input("Leitura Anterior", value=None, format="DD/MM/YYYY")
        prox_leit_val = c6.date_input("Próxima Leitura", value=None, format="DD/MM/YYYY")
        
        st.divider()
        st.markdown("##### ⚡ 1. Energia Ativa (kWh) e Demanda Registrada (kW)")
        col1, col2, col3, col4 = st.columns(4)
        
        label_help_ponta_c = "Bloqueado automaticamente pois a Tarifa B3 possui consumo único." if is_b3 else ""
        q_c_p = col1.number_input("Consumo Ponta (TUSD/TE)", min_value=0.0, format="%.4f", disabled=is_b3, help=label_help_ponta_c)
        
        label_cons_fp = "Consumo Único (kWh)" if is_b3 else "Consumo F. Ponta (TUSD/TE)"
        q_c_fp = col2.number_input(label_cons_fp, min_value=0.0, format="%.4f")
        
        label_help_d_p = "Bloqueado para B3 e Verde." if (is_verde or is_b3) else ""
        q_d_reg_p = col3.number_input("Demanda Registrada Ponta", min_value=0.0, format="%.4f", disabled=(is_verde or is_b3), help=label_help_d_p)
        
        label_dem_fp = "Demanda Registrada ÚNICA (kW)" if is_verde else ("Demanda Registrada F. Ponta" if not is_b3 else "Não se aplica (B3)")
        q_d_reg_fp = col4.number_input(label_dem_fp, min_value=0.0, format="%.4f", disabled=is_b3)
        
        st.divider()
        st.markdown("##### 🔌 2. Energia e Demanda Reativa")
        col5, col6, col7, col8 = st.columns(4)
        q_r_p = col5.number_input("Consumo Reat. Ponta (kvarh)", min_value=0.0, format="%.4f", disabled=is_b3)
        q_r_fp = col6.number_input("Consumo Reat. F. Ponta", min_value=0.0, format="%.4f", disabled=is_b3)
        q_dr_p = col7.number_input("Demanda Reat. Ponta (kW)", min_value=0.0, format="%.4f", disabled=(is_verde or is_b3))
        q_dr_fp = col8.number_input("Demanda Reat. ÚNICA / F. Ponta", min_value=0.0, format="%.4f", disabled=is_b3)
        
        st.divider()
        st.markdown("##### 💸 3. Retenções de Impostos (R$)")
        c7, c8 = st.columns(2) 
        ret_cons_irrf = c7.number_input("Retenção Consumo IRRF (-)", min_value=0.0, format="%.2f")
        ret_dem_irrf = c8.number_input("Retenção Demanda IRRF (-)", min_value=0.0, format="%.2f")
        
        st.write("")
        if 'mensagem_sucesso' in st.session_state:
            st.success(st.session_state['mensagem_sucesso'])
            del st.session_state['mensagem_sucesso']
        
        submit = st.form_submit_button("Registrar Fatura", type="primary")
        
        if submit:
            if not uc_input or not mes or not vencimento_val or not p_inicio_val or not p_fim_val or not prox_leit_val:
                st.warning("⚠️ Atenção: Por favor, preencha a UC (no campo externo), o Mês de Referência e TODAS as Datas antes de gerar a fatura.")
            
            elif not re.match(r"^[A-Z]{3}/\d{4}$", mes):
                st.error("⚠️ Erro de Formatação: O Mês de Referência deve ter 3 letras, uma barra e 4 números. Exemplo: DEZ/2025 ou JAN/2026.")
            
            else:
                conexao = obter_conexao()
                c_dup = conexao.cursor()
                c_dup.execute("SELECT id FROM faturas_cpfl WHERE unidade_consumidora = %s AND mes_referencia = %s", (uc_input, mes))
                
                if c_dup.fetchone():
                    st.warning(f"⚠️ A fatura da UC {uc_input} referente a {mes} já existe no banco de dados! Inserção cancelada.")
                    conexao.close()
                else:
                    conexao.close() 
                    
                    vencimento = vencimento_val.strftime("%d/%m/%Y")
                    p_inicio = p_inicio_val.strftime("%d/%m/%Y")
                    p_fim = p_fim_val.strftime("%d/%m/%Y")
                    prox_leit = prox_leit_val.strftime("%d/%m/%Y")

                    calc = calcular_faturamento(uc_input, mes, q_c_p, q_c_fp, q_d_reg_p, q_d_reg_fp, q_r_p, q_r_fp, q_dr_p, q_dr_fp, ret_cons_irrf, ret_dem_irrf)
                    
                    if "erro" in calc:
                        st.error(calc["erro"])
                    else:
                        conexao = obter_conexao()
                        c = conexao.cursor()
                        
                        interrogacoes = ', '.join(['%s'] * 74)
                        
                        c.execute(f'''
                            INSERT INTO faturas_cpfl (
                                unidade_consumidora, nome_unidade, atividade, mes_referencia, data_vencimento, periodo_leitura_inicio, periodo_leitura_fim, data_proxima_leitura,
                                classificacao, demanda_contratada_ponta, demanda_contratada_fponta,
                                
                                consumo_ponta, tarifa_aneel_cons_ponta_tusd, tarifa_trib_cons_ponta_tusd, valor_cons_ponta_tusd,
                                consumo_fora_ponta, tarifa_aneel_cons_fponta_tusd, tarifa_trib_cons_fponta_tusd, valor_cons_fponta_tusd,
                                tarifa_aneel_cons_ponta_te, tarifa_trib_cons_ponta_te, valor_cons_ponta_te,
                                tarifa_aneel_cons_fponta_te, tarifa_trib_cons_fponta_te, valor_cons_fponta_te,
                                
                                tipo_bandeira, adicional_bandeira,
                                
                                demanda_isenta_ponta, tarifa_aneel_dem_isenta_ponta, tarifa_trib_dem_isenta_ponta, valor_dem_isenta_ponta,
                                demanda_registrada_ponta, tarifa_aneel_dem_ponta, tarifa_trib_dem_ponta, valor_dem_ponta,
                                demanda_isenta_fora_ponta, tarifa_aneel_dem_isenta_fponta, tarifa_trib_dem_isenta_fponta, valor_dem_isenta_fponta,
                                demanda_registrada_fora_ponta, tarifa_aneel_dem_fponta, tarifa_trib_dem_fponta, valor_dem_fponta,
                                
                                demanda_ultrapassagem_ponta, tarifa_aneel_dem_ultrap_ponta, tarifa_trib_dem_ultrap_ponta, valor_dem_ultrap_ponta,
                                demanda_ultrapassagem_fora_ponta, tarifa_aneel_dem_ultrap_fponta, tarifa_trib_dem_ultrap_fponta, valor_dem_ultrap_fponta,
                                
                                consumo_reativo_ponta, tarifa_aneel_cons_reativo_ponta, tarifa_trib_cons_reativo_ponta, valor_cons_reativo_ponta,
                                consumo_reativo_fora_ponta, tarifa_aneel_cons_reativo_fponta, tarifa_trib_cons_reativo_fponta, valor_cons_reativo_fponta,
                                demanda_reativa_ponta, tarifa_aneel_dem_reativa_ponta, tarifa_trib_dem_reativa_ponta, valor_dem_reativa_ponta,
                                demanda_reativa_fora_ponta, tarifa_aneel_dem_reativa_fponta, tarifa_trib_dem_reativa_fponta, valor_dem_reativa_fponta,
                                
                                cip, retencao_consumo_irrf, retencao_demanda_irrf, valor_total_pis, valor_total_cofins, valor_total_icms, valor_total_fatura
                            ) VALUES ({interrogacoes})
                        ''', (
                            uc_input, calc["nome_unidade"], calc["atividade"], mes, vencimento, p_inicio, p_fim, prox_leit,
                            calc["classificacao"], calc["demanda_contratada_ponta"], calc["demanda_contratada_fponta"],
                            
                            q_c_p, calc["ta_c_p_tusd"], calc["tt_c_p_tusd"], calc["v_c_p_tusd"],
                            q_c_fp, calc["ta_c_fp_tusd"], calc["tt_c_fp_tusd"], calc["v_c_fp_tusd"],
                            calc["ta_c_p_te"], calc["tt_c_p_te"], calc["v_c_p_te"],
                            calc["ta_c_fp_te"], calc["tt_c_fp_te"], calc["v_c_fp_te"],
                            
                            calc["tipo_bandeira"], calc["adicional_bandeira"],
                            
                            calc["q_d_ise_p"], calc["ta_d_p"], calc["tt_d_ise_p"], calc["v_d_ise_p"],
                            calc["q_d_reg_p"], calc["ta_d_p"], calc["tt_d_p"], calc["v_d_reg_p"],
                            calc["q_d_ise_fp"], calc["ta_d_fp"], calc["tt_d_ise_fp"], calc["v_d_ise_fp"],
                            calc["q_d_reg_fp"], calc["ta_d_fp"], calc["tt_d_fp"], calc["v_d_reg_fp"],
                            
                            calc["q_up_p"], calc["ta_up_p"], calc["tt_up_p"], calc["v_up_p"],
                            calc["q_up_fp"], calc["ta_up_fp"], calc["tt_up_fp"], calc["v_up_fp"],
                            
                            q_r_p, calc["ta_r_p"], calc["tt_r_p"], calc["v_r_p"],
                            q_r_fp, calc["ta_r_fp"], calc["tt_r_fp"], calc["v_r_fp"],
                            q_dr_p, calc["ta_dr_p"], calc["tt_dr_p"], calc["v_dr_p"],
                            q_dr_fp, calc["ta_dr_fp"], calc["tt_dr_fp"], calc["v_dr_fp"],
                            
                            calc["cip_padrao"], ret_cons_irrf, ret_dem_irrf, calc["v_pis"], calc["v_cofins"], calc["v_icms"], calc["valor_total_fatura"]
                        ))
                        conexao.commit()
                        carregar_dados.clear()
                        conexao.close()
                        
                        st.session_state['mensagem_sucesso'] = f"✅ Fatura calculada e salva com sucesso! Valor Estimado: R$ {calc['valor_total_fatura']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                        st.session_state.form_key += 1
                        st.rerun()

# ==========================================
# ABA CONFIGURAÇÕES DE CADASTRO E TARIFAS
# ==========================================
with aba_config:
    st.markdown("##### ⚙️ Configurações Gerais do Sistema")
    
    col_cad, col_tar = st.columns(2)
    
    with col_cad:
        st.markdown("###### 🏢 Cadastro da Unidade Consumidora")
        
        tab_cad_manual, tab_cad_lote = st.tabs(["✍️ Cadastro Manual", "📊 Upload em Lote (Excel)"])
        
        with tab_cad_manual:
            uc_busca = st.text_input("Buscar UC (Ex: 40190245)", value="").strip()
            
            if uc_busca:
                conexao = obter_conexao()
                c = conexao.cursor()
                c.execute("SELECT nome_unidade, atividade, classificacao, demanda_contratada_ponta, demanda_contratada_fponta FROM cadastro_uc WHERE unidade_consumidora = %s", (uc_busca,))
                dados_uc = c.fetchone()
                conexao.close()
            else:
                dados_uc=None
            
            if dados_uc:
                v_nome, v_ativ, v_class, v_dc_p, v_dc_fp = dados_uc
            else:
                v_nome, v_ativ, v_class, v_dc_p, v_dc_fp = ("", "Administrativa", "Tarifa Azul-A4", 0.0, 0.0)
                
            with st.form("form_uc"):
                nome_input = st.text_input("Nome da Instalação/Unidade", value=v_nome, placeholder="Ex: Poço 15 - Geisel")
                
                lista_atividades = ["Administrativa", "Água", "Esgoto"]
                idx_ativ = lista_atividades.index(v_ativ) if v_ativ in lista_atividades else 0
                ativ_input = st.selectbox("Atividade", lista_atividades, index=idx_ativ)
                
                lista_classes = ["Tarifa Azul-A4", "Tarifa Verde-A4", "Convencional B3"]
                idx_class = lista_classes.index(v_class) if v_class in lista_classes else 0
                classif_input = st.selectbox("Classificação", lista_classes, index=idx_class)
                
                if "Verde" in classif_input:
                    st.info("💡 Na **Tarifa Verde-A4**, informe a Demanda Única no campo 'Fora Ponta'. O campo Ponta será desconsiderado no cálculo.")
                elif "B3" in classif_input:
                    st.info("💡 Na **Convencional B3**, não existe demanda contratada. Pode deixar os campos zerados.")
                
                dc_p = st.number_input("Demanda Contratada Ponta (kW)", value=float(v_dc_p), format="%.2f", disabled=("B3" in classif_input))
                dc_fp = st.number_input("Demanda Contratada Fora Ponta (kW)", value=float(v_dc_fp), format="%.2f", disabled=("B3" in classif_input))
                
                st.write("")
                if 'msg_uc' in st.session_state:
                    st.success(st.session_state['msg_uc'])
                    del st.session_state['msg_uc']
                
                if st.form_submit_button("Salvar Cadastro da UC", type="primary"):
                    if classif_input == "Tarifa Verde-A4":
                        dc_p = 0.0 
                    elif classif_input == "Convencional B3":
                        dc_p = 0.0
                        dc_fp = 0.0
                    
                    conexao = obter_conexao()
                    c = conexao.cursor()
                    c.execute('''
                        INSERT INTO cadastro_uc (unidade_consumidora, nome_unidade, atividade, classificacao, demanda_contratada_ponta, demanda_contratada_fponta)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (unidade_consumidora) DO UPDATE SET 
                        nome_unidade = EXCLUDED.nome_unidade,
                        atividade = EXCLUDED.atividade,
                        classificacao = EXCLUDED.classificacao,
                        demanda_contratada_ponta = EXCLUDED.demanda_contratada_ponta,
                        demanda_contratada_fponta = EXCLUDED.demanda_contratada_fponta;
                    ''', (uc_busca, nome_input, ativ_input, classif_input, dc_p, dc_fp))
                    conexao.commit()
                    conexao.close()
                    
                    st.session_state['msg_uc'] = "✅ Cadastro da UC realizado com sucesso!"
                    st.rerun()

        with tab_cad_lote:
            st.info("💡 Crie uma planilha no Excel com o cabeçalho idêntico à tabela abaixo e faça o upload.")
            
            df_exemplo = pd.DataFrame({
                "unidade_consumidora": ["40190245", "4065530"],
                "nome_unidade": ["ETA Bauru", "Promocao Social"],
                "atividade": ["Água", "Administrativa"],
                "classificacao": ["Tarifa Azul-A4", "Convencional B3"],
                "demanda_contratada_ponta": [275.0, 0.0],
                "demanda_contratada_fponta": [275.0, 0.0]
            })
            
            st.dataframe(df_exemplo, hide_index=True, use_container_width=True)
            
            arquivo_ucs_excel = st.file_uploader("Selecione a planilha de UCs (.xlsx)", type=["xlsx"], key="upload_uc")
            
            if arquivo_ucs_excel is not None:
                if st.button("🚀 Processar e Cadastrar Unidades", type="primary"):
                    try:
                        df_ucs = pd.read_excel(arquivo_ucs_excel)
                        conexao = obter_conexao()
                        c = conexao.cursor()
                        
                        inseridas = 0
                        
                        for index, row in df_ucs.iterrows():
                            uc = str(row.get('unidade_consumidora', '')).strip()
                            
                            if not uc or uc == 'nan':
                                continue
                            
                            nome = str(row.get('nome_unidade', '')).strip()
                            ativ = str(row.get('atividade', 'Administrativa')).strip()
                            classif = str(row.get('classificacao', 'Tarifa Azul-A4')).strip()
                            
                            try:
                                dc_p = float(row.get('demanda_contratada_ponta', 0.0))
                            except:
                                dc_p = 0.0
                                
                            try:
                                dc_fp = float(row.get('demanda_contratada_fponta', 0.0))
                            except:
                                dc_fp = 0.0

                            if "Verde" in classif:
                                dc_p = 0.0
                            elif "B3" in classif:
                                dc_p = 0.0
                                dc_fp = 0.0
                                
                            c.execute('''
                                INSERT INTO cadastro_uc (unidade_consumidora, nome_unidade, atividade, classificacao, demanda_contratada_ponta, demanda_contratada_fponta)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (unidade_consumidora) DO UPDATE SET 
                                nome_unidade = EXCLUDED.nome_unidade,
                                atividade = EXCLUDED.atividade,
                                classificacao = EXCLUDED.classificacao,
                                demanda_contratada_ponta = EXCLUDED.demanda_contratada_ponta,
                                demanda_contratada_fponta = EXCLUDED.demanda_contratada_fponta;
                            ''', (uc, nome, ativ, classif, dc_p, dc_fp))
                            inseridas += 1
                            
                        conexao.commit()
                        conexao.close()
                        
                        st.success(f"✅ Lote processado! **{inseridas}** UCs cadastradas ou atualizadas no sistema.")
                        st.balloons()
                        
                    except Exception as e:
                        st.error(f"Erro ao processar a planilha. Verifique se o nome das colunas está correto. Detalhe: {e}")

    with col_tar:
        st.markdown("###### 📈 Tarifas e Impostos")
        
        c_mes, c_class = st.columns(2)
        mes_edicao = c_mes.text_input("Mês Referência", value="DEZ/2025").strip().upper()
        class_edicao = c_class.selectbox("Classe Tarifária", ["Tarifa Azul-A4", "Tarifa Verde-A4", "Convencional B3"])

        conexao = obter_conexao()
        c = conexao.cursor()
        c.execute("SELECT * FROM parametros_faturamento WHERE mes_referencia = %s AND classificacao = %s", (mes_edicao, class_edicao))
        params_atuais = c.fetchone()

        if params_atuais:
            _, _, p_pis, p_cofins, p_icms, p_t_c_p_tusd, p_t_c_fp_tusd, p_t_c_p_te, p_t_c_fp_te, p_t_d_p, p_t_d_fp, p_t_reat, p_t_band, p_cip, p_band_tipo = params_atuais
            st.info(f"Mostrando os parâmetros já cadastrados para **{mes_edicao} ({class_edicao})**.")
        else:
            c.execute("SELECT * FROM parametros_faturamento WHERE classificacao = %s", (class_edicao,))
            todos_params = c.fetchall()
            if todos_params:
                ultimo = todos_params[-1]
                _, _, p_pis, p_cofins, p_icms, p_t_c_p_tusd, p_t_c_fp_tusd, p_t_c_p_te, p_t_c_fp_te, p_t_d_p, p_t_d_fp, p_t_reat, p_t_band, p_cip, p_band_tipo = ultimo
                st.warning(f"Mês novo detectado! Herdando automaticamente as tarifas de **{ultimo[0]} ({class_edicao})**.")
            else:
                p_pis, p_cofins, p_icms, p_t_c_p_tusd, p_t_c_fp_tusd, p_t_c_p_te, p_t_c_fp_te, p_t_d_p, p_t_d_fp, p_t_reat, p_t_band, p_cip = (0.0,)*12
                p_band_tipo = "VERDE"
                
        conexao.close()

        with st.form(f"form_config_{mes_edicao}_{class_edicao}"):
            st.markdown("**Alíquotas de Impostos (em decimal)**")
            c1, c2, c3 = st.columns(3)
            novo_pis = c1.number_input("PIS", value=float(p_pis), format="%.4f")
            novo_cofins = c2.number_input("COFINS", value=float(p_cofins), format="%.4f")
            novo_icms = c3.number_input("ICMS", value=float(p_icms), format="%.4f")

            st.markdown("**Tarifas ANEEL, Bandeira e CIP (R$)**")
            
            if class_edicao == "Convencional B3":
                st.caption("ℹ️ Para Convencional B3, cadastre a Tarifa Única nos campos *Consumo F. Ponta*. Campos de Demanda e Reativo podem ficar zerados.")
            
            c4, c5 = st.columns(2)
            novo_t_c_p_tusd = c4.number_input("Consumo Ponta TUSD", value=float(p_t_c_p_tusd), format="%.8f")
            novo_t_c_fp_tusd = c5.number_input("Consumo F. Ponta TUSD", value=float(p_t_c_fp_tusd), format="%.8f")
            novo_t_c_p_te = c4.number_input("Consumo Ponta TE", value=float(p_t_c_p_te), format="%.8f")
            novo_t_c_fp_te = c5.number_input("Consumo F. Ponta TE", value=float(p_t_c_fp_te), format="%.8f")
            
            c6, c7, c8 = st.columns(3)
            if class_edicao == "Tarifa Verde-A4":
                st.caption("ℹ️ Para a Tarifa Verde, cadastre a Demanda Única no campo de *Demanda F. Ponta*.")
            
            novo_t_d_p = c6.number_input("Demanda Ponta", value=float(p_t_d_p), format="%.8f")
            novo_t_d_fp = c7.number_input("Demanda F. Ponta", value=float(p_t_d_fp), format="%.8f")
            novo_t_reat = c8.number_input("Energia Reativa", value=float(p_t_reat), format="%.8f")
            
            c9, c10, c11 = st.columns(3)
            lista_bandeiras = ["VERDE", "AMARELA", "VERMELHA I", "VERMELHA II", "ESCASSEZ HÍDRICA"]
            idx_band = lista_bandeiras.index(p_band_tipo) if p_band_tipo in lista_bandeiras else 0
            novo_tipo_bandeira = c9.selectbox("Bandeira Vigente do Mês", lista_bandeiras, index=idx_band)
            
            novo_t_band = c10.number_input("Tarifa Band. (R$/kWh)", value=float(p_t_band), format="%.5f")
            novo_cip = c11.number_input("CIP Padronizada (R$)", value=float(p_cip), format="%.2f")

            st.write("")
            if 'msg_tarifa' in st.session_state:
                st.success(st.session_state['msg_tarifa'])
                del st.session_state['msg_tarifa']

            if st.form_submit_button("Salvar Parâmetros", type="primary"):
                conexao = obter_conexao()
                c = conexao.cursor()
                c.execute('''
                    INSERT INTO parametros_faturamento VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (mes_referencia, classificacao) DO UPDATE SET
                    aliq_pis = EXCLUDED.aliq_pis, aliq_cofins = EXCLUDED.aliq_cofins, aliq_icms = EXCLUDED.aliq_icms,
                    tar_aneel_cons_p_tusd = EXCLUDED.tar_aneel_cons_p_tusd, tar_aneel_cons_fp_tusd = EXCLUDED.tar_aneel_cons_fp_tusd,
                    tar_aneel_cons_p_te = EXCLUDED.tar_aneel_cons_p_te, tar_aneel_cons_fp_te = EXCLUDED.tar_aneel_cons_fp_te,
                    tar_aneel_dem_p = EXCLUDED.tar_aneel_dem_p, tar_aneel_dem_fp = EXCLUDED.tar_aneel_dem_fp,
                    tar_aneel_reativo = EXCLUDED.tar_aneel_reativo, tar_bandeira_vigente = EXCLUDED.tar_bandeira_vigente,
                    cip_padrao = EXCLUDED.cip_padrao, tipo_bandeira = EXCLUDED.tipo_bandeira;
                ''', (
                    mes_edicao, class_edicao, novo_pis, novo_cofins, novo_icms,
                    novo_t_c_p_tusd, novo_t_c_fp_tusd, novo_t_c_p_te, novo_t_c_fp_te,
                    novo_t_d_p, novo_t_d_fp, novo_t_reat, novo_t_band, novo_cip, novo_tipo_bandeira
                ))
                conexao.commit()
                conexao.close()
                
                st.session_state['msg_tarifa'] = f"✅ Parâmetros de **{class_edicao}** atualizados para {mes_edicao}!"
                st.rerun()
