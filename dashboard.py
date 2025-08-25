import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(layout="wide", page_title="Dashboard de Clientes")

# --- ESTILOS CSS CUSTOMIZADOS ---
st.markdown("""
<style>
    [data-testid="stMetric"] {
        background-color: #2a2a39;
        border: 1px solid #4f4f6b;
        padding: 15px;
        border-radius: 10px;
        color: white;
    }
    [data-testid="stMetricValue"] {
        font-size: 2.2rem;
        font-weight: 600;
        color: #22a8e0;
    }
    [data-testid="stMetricLabel"] {
        font-size: 1rem;
        color: #a0a0b8;
    }
</style>
""", unsafe_allow_html=True)


# --- FUNÇÕES DE PROCESSAMENTO DE DADOS ---

def formatar_valor_brl(valor):
    """Formata um número para o padrão monetário brasileiro (R$)."""
    if pd.isna(valor):
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def limpar_valor_monetario(valor):
    """Converte uma string monetária (ex: 'R$ 1.234,56') para float."""
    if isinstance(valor, (int, float)):
        return valor
    if isinstance(valor, str):
        valor_limpo = valor.replace("R$", "").strip().replace(".", "").replace(",", ".")
        try:
            return float(valor_limpo)
        except (ValueError, TypeError):
            return 0.0
    return 0.0

def identificar_tipo_opcao(ticker):
    """Identifica se uma opção é CALL ou PUT com base no ticker (padrão B3)."""
    if not isinstance(ticker, str) or len(ticker) < 5:
        return 'N/D'
    quinta_letra = ticker[4].upper()
    if 'A' <= quinta_letra <= 'L':
        return 'Call'
    elif 'M' <= quinta_letra <= 'X':
        return 'Put'
    else:
        return 'N/D'

@st.cache_data(ttl=600) # Cache de 10 minutos
def carregar_dados_sheets():
    """
    Função OTIMIZADA e segura para ler e processar a Planilha Google.
    Usa um número mínimo de chamadas de API para máxima performance.
    """
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], scopes=scopes
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url(st.secrets["private_gsheets_url"])
        
        # --- Leitura OTIMIZADA de todas as abas de uma vez ---
        worksheets = spreadsheet.worksheets()
        all_sheets_data = {}
        for sheet in worksheets:
            all_sheets_data[sheet.title] = sheet.get_all_values()

        # --- Processamento da aba 'Clientes' ---
        sheet_clientes_data = all_sheets_data.get("Clientes", [])
        if not sheet_clientes_data:
             st.error("Aba 'Clientes' não encontrada na Planilha Google.")
             return pd.DataFrame(), {}
        
        df_clientes = pd.DataFrame(sheet_clientes_data[1:], columns=sheet_clientes_data[0])
        df_clientes['Início do Acompanhamento'] = pd.to_datetime(df_clientes['Início do Acompanhamento'], errors='coerce')

        # --- Processamento das abas individuais ---
        nomes_clientes = df_clientes['Nome'].tolist()
        dados_completos_clientes = {}
        meses_pt = ['JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO', 'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO']

        for nome in nomes_clientes:
            if nome in all_sheets_data:
                data = all_sheets_data[nome]
                df_cliente_raw = pd.DataFrame(data).fillna('')
                
                # Processamento da carteira de investimentos
                try:
                    start_row_inv = df_cliente_raw[df_cliente_raw[0] == 'CÓDIGO'].index[0]
                    header_inv = df_cliente_raw.iloc[start_row_inv].tolist()[:4]
                    data_inv = df_cliente_raw.iloc[start_row_inv + 1:].iloc[:, :4]
                    df_investimentos = pd.DataFrame(data_inv.values, columns=header_inv)
                    df_investimentos.columns = ['Código', 'Quantidade', 'Preço Médio', 'Valor Investido']
                    df_investimentos.dropna(how='all', inplace=True)
                    df_investimentos['Quantidade'] = pd.to_numeric(df_investimentos['Quantidade'], errors='coerce').round().astype('Int64')
                    df_investimentos['Valor Investido'] = df_investimentos['Valor Investido'].apply(limpar_valor_monetario)
                    df_investimentos['Preço Médio'] = df_investimentos['Preço Médio'].apply(limpar_valor_monetario)
                except (IndexError, ValueError):
                    df_investimentos = pd.DataFrame()

                # Processamento da carteira de opções
                lista_df_opcoes = []
                month_rows_indices = df_cliente_raw[df_cliente_raw.apply(lambda r: any(str(c).upper() in meses_pt for c in r), axis=1)].index.tolist()
                
                for i, start_block_idx in enumerate(month_rows_indices):
                    # ... (lógica de extração de opções permanece a mesma)
                    end_block_idx = month_rows_indices[i + 1] if i + 1 < len(month_rows_indices) else len(df_cliente_raw)
                    month_row = df_cliente_raw.loc[start_block_idx]
                    mes_atual = next((str(c).capitalize() for c in month_row if str(c).upper() in meses_pt), None)
                    df_search_area = df_cliente_raw.loc[start_block_idx:end_block_idx-1]
                    header_row_series = df_search_area[df_search_area.apply(lambda r: 'SITUAÇÃO' in r.astype(str).values, axis=1)]
                    
                    if not header_row_series.empty:
                        header_idx = header_row_series.index[0]
                        header_content = df_cliente_raw.loc[header_idx]
                        start_col_op = header_content[header_content.astype(str) == 'SITUAÇÃO'].index[0]
                        data_rows = df_cliente_raw.loc[header_idx + 1: end_block_idx - 1]
                        df_temp = data_rows.iloc[:, start_col_op:start_col_op + 7]
                        df_temp.columns = ['Situação', 'Ativo', 'Opção', 'Strike', 'Recomendação', 'Quantidade', 'Preço Executado']
                        df_temp = df_temp[df_temp['Situação'] != ''].dropna(how='all')
                        if not df_temp.empty:
                            df_temp['Mês'] = mes_atual
                            lista_df_opcoes.append(df_temp)

                if lista_df_opcoes:
                    df_opcoes_final = pd.concat(lista_df_opcoes, ignore_index=True)
                    df_opcoes_final['Quantidade'] = pd.to_numeric(df_opcoes_final['Quantidade'], errors='coerce').round().astype('Int64')
                    df_opcoes_final['Strike'] = df_opcoes_final['Strike'].apply(limpar_valor_monetario)
                    df_opcoes_final['Preço Executado'] = df_opcoes_final['Preço Executado'].apply(limpar_valor_monetario)
                    df_opcoes_final['Tipo'] = df_opcoes_final['Opção'].apply(identificar_tipo_opcao)
                else:
                    df_opcoes_final = pd.DataFrame()

                dados_completos_clientes[nome] = {
                    'investimentos': df_investimentos,
                    'opcoes': df_opcoes_final
                }
            else:
                st.warning(f"Aba para o cliente '{nome}' não encontrada na Planilha Google.")
                dados_completos_clientes[nome] = {'investimentos': pd.DataFrame(), 'opcoes': pd.DataFrame()}
        
        return df_clientes, dados_completos_clientes

    except Exception as e:
        st.error(f"Erro ao conectar com o Google Sheets. Verifique seus 'Secrets' e as permissões da planilha. Detalhes: {e}")
        return pd.DataFrame(), {}

# --- INÍCIO DO DASHBOARD ---
st.title("📈 Dashboard de Acompanhamento de Clientes")
st.markdown("Use o menu na lateral para navegar entre as seções.")

df_clientes, dados_carteiras = carregar_dados_sheets()

if df_clientes.empty:
    st.warning("Nenhum dado de cliente para exibir. Verifique a conexão com o Google Sheets e o conteúdo da planilha.")
    st.stop()

# --- BARRA LATERAL DE NAVEGAÇÃO ---
st.sidebar.image("https://i.ibb.co/ymrwQqB1/230x0w.webp", width=100)
st.sidebar.title("Menu de Navegação")
pagina_selecionada = st.sidebar.radio(
    "Selecione uma seção:",
    ("📊 Visão Geral", "💰 Carteira de Investimentos", "📈 Carteira de Opções")
)

# --- SEÇÃO: VISÃO GERAL ---
if pagina_selecionada == "📊 Visão Geral":
    st.header("📊 Visão Geral dos Clientes")

    patrimonio_total = sum(
        dados_carteiras[nome]['investimentos']['Valor Investido'].sum()
        for nome in df_clientes['Nome'] if nome in dados_carteiras and not dados_carteiras[nome]['investimentos'].empty
    )
    total_clientes = len(df_clientes)
    
    col1, col2 = st.columns(2)
    col1.metric(label="Total de Clientes", value=total_clientes)
    col2.metric(label="Patrimônio Total Investido", value=formatar_valor_brl(patrimonio_total))
    st.markdown("---")

    col_graf1, col_graf2 = st.columns(2)
    with col_graf1:
        fig_plano = px.pie(df_clientes, names='Plano', title='Distribuição de Clientes por Plano', hole=0.4, color_discrete_sequence=px.colors.sequential.Blues_r)
        st.plotly_chart(fig_plano, use_container_width=True)
    with col_graf2:
        df_clientes_por_data = df_clientes.dropna(subset=['Início do Acompanhamento']).set_index('Início do Acompanhamento').resample('ME').size().reset_index(name='Novos Clientes')
        fig_evolucao = px.line(df_clientes_por_data, x='Início do Acompanhamento', y='Novos Clientes', title='Evolução de Inícios de Acompanhamento', markers=True)
        st.plotly_chart(fig_evolucao, use_container_width=True)

    st.subheader("Lista de Clientes")
    df_clientes_display = df_clientes.copy()
    df_clientes_display.index = range(1, len(df_clientes_display) + 1)
    st.dataframe(df_clientes_display.style.format({"Início do Acompanhamento": lambda x: x.strftime('%d/%m/%Y') if pd.notna(x) else ''}), use_container_width=True)

# --- SEÇÃO: CARTEIRA DE INVESTIMENTOS ---
elif pagina_selecionada == "💰 Carteira de Investimentos":
    st.header("💰 Análise da Carteira de Investimentos")
    
    cliente_selecionado = st.sidebar.selectbox("Selecione um Cliente", options=df_clientes['Nome'].unique())
    st.sidebar.caption("Clique na caixa e digite para pesquisar.")
    
    if cliente_selecionado:
        df_invest = dados_carteiras.get(cliente_selecionado, {}).get('investimentos', pd.DataFrame())
        if df_invest.empty:
            st.warning(f"O cliente '{cliente_selecionado}' não possui dados de investimentos registrados.")
        else:
            patrimonio_cliente = df_invest['Valor Investido'].sum()
            num_ativos = len(df_invest)
            maior_posicao_valor = df_invest['Valor Investido'].max()
            maior_posicao_ativo = df_invest.loc[df_invest['Valor Investido'].idxmax()]['Código']

            col1, col2, col3 = st.columns(3)
            col1.metric("Patrimônio Total do Cliente", formatar_valor_brl(patrimonio_cliente))
            col2.metric("Número de Ativos", num_ativos)
            col3.metric("Maior Posição", f"{maior_posicao_ativo} ({formatar_valor_brl(maior_posicao_valor)})")
            st.markdown("---")

            col_graf1, col_graf2 = st.columns([0.4, 0.6])
            with col_graf1:
                fig_composicao = px.pie(df_invest, names='Código', values='Valor Investido', title=f'Composição da Carteira de {cliente_selecionado}', hole=0.4)
                st.plotly_chart(fig_composicao, use_container_width=True)
            with col_graf2:
                fig_barras = px.bar(df_invest.sort_values('Valor Investido', ascending=False), x='Código', y='Valor Investido', title=f'Valor Investido por Ativo', text_auto='.2s')
                st.plotly_chart(fig_barras, use_container_width=True)

            with st.expander("Ver tabela detalhada da carteira"):
                df_invest_display = df_invest.copy()
                df_invest_display.index = range(1, len(df_invest_display) + 1)
                st.dataframe(df_invest_display.style.format({'Preço Médio': formatar_valor_brl, 'Valor Investido': formatar_valor_brl}), use_container_width=True)

# --- SEÇÃO: CARTEIRA DE OPÇÕES ---
elif pagina_selecionada == "📈 Carteira de Opções":
    st.header("📈 Análise da Carteira de Opções")

    cliente_selecionado_op = st.sidebar.selectbox("Selecione um Cliente", options=df_clientes['Nome'].unique(), key="cliente_opcoes")
    st.sidebar.caption("Clique na caixa e digite para pesquisar.")

    if cliente_selecionado_op:
        df_opcoes = dados_carteiras.get(cliente_selecionado_op, {}).get('opcoes', pd.DataFrame())

        if df_opcoes.empty:
            st.warning(f"O cliente '{cliente_selecionado_op}' não possui dados de opções registrados.")
        else:
            meses_disponiveis = sorted(df_opcoes['Mês'].dropna().unique())
            mes_filtrado = st.sidebar.selectbox("Filtrar por Mês", options=meses_disponiveis)
            
            df_mes_filtrado = df_opcoes[df_opcoes['Mês'] == mes_filtrado]

            col_filtro1, col_filtro2 = st.columns(2)
            with col_filtro1:
                ativos_disponiveis = sorted(df_mes_filtrado['Ativo'].dropna().unique())
                ativo_filtrado = st.multiselect("Filtrar por Ativo", options=ativos_disponiveis, default=ativos_disponiveis)
            with col_filtro2:
                situacoes_disponiveis = sorted(df_mes_filtrado['Situação'].dropna().unique())
                situacao_filtrada = st.multiselect("Filtrar por Situação", options=situacoes_disponiveis, default=situacoes_disponiveis)
            
            df_filtrada = df_mes_filtrado[
                df_mes_filtrado['Ativo'].isin(ativo_filtrado) &
                df_mes_filtrado['Situação'].isin(situacao_filtrada)
            ]
            
            if df_filtrada.empty:
                st.info("Nenhum dado corresponde aos filtros selecionados.")
            else:
                resumo_situacao = df_filtrada['Situação'].value_counts().rename_axis('Situação').reset_index(name='Contagem')
                
                risco_calls = df_filtrada[df_filtrada['Tipo'] == 'Call']['Quantidade'].sum()
                risco_puts = df_filtrada[df_filtrada['Tipo'] == 'Put']['Quantidade'].sum()

                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("Resumo por Situação")
                    resumo_situacao_display = resumo_situacao.copy()
                    resumo_situacao_display.index = range(1, len(resumo_situacao_display) + 1)
                    st.dataframe(resumo_situacao_display, use_container_width=True)
                with col2:
                    st.subheader("Indicador de Risco")
                    st.metric("Proporção Calls vs Puts", f"{int(risco_calls)} / {int(risco_puts)}")
                    st.caption("Quantidade de Calls vs Puts na carteira filtrada.")

                st.markdown("---")

                col_graf1, col_graf2 = st.columns(2)
                with col_graf1:
                    df_qtd_ativo = df_filtrada.groupby('Ativo')['Quantidade'].sum().reset_index()
                    fig_qtd = px.bar(df_qtd_ativo, x='Ativo', y='Quantidade', title='Quantidade Total por Ativo')
                    st.plotly_chart(fig_qtd, use_container_width=True)
                with col_graf2:
                    fig_scatter = px.scatter(df_filtrada, x='Strike', y='Preço Executado', color='Ativo', hover_data=['Opção', 'Situação', 'Tipo'], title='Strike vs. Preço Executado')
                    st.plotly_chart(fig_scatter, use_container_width=True)

                with st.expander("Ver tabela detalhada de opções"):
                    df_filtrada_display = df_filtrada.copy()
                    df_filtrada_display.index = range(1, len(df_filtrada_display) + 1)
                    st.dataframe(df_filtrada_display.style.format({'Strike': formatar_valor_brl, 'Preço Executado': formatar_valor_brl}), use_container_width=True)

# --- Seção de Rodapé ---
st.sidebar.markdown("---")
st.sidebar.info("Dashboard desenvolvido para gestão de carteiras. v1.7")