import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(layout="wide", page_title="Dashboard de Clientes")

# --- ESTILOS CSS CUSTOMIZADOS ---
st.markdown("""
<style>
    [data-testid="stMetric"] {
        background-color: #2a2a39; border: 1px solid #4f4f6b; padding: 15px;
        border-radius: 10px; color: white;
    }
    [data-testid="stMetricValue"] { font-size: 2.2rem; font-weight: 600; color: #22a8e0; }
    [data-testid="stMetricLabel"] { font-size: 1rem; color: #a0a0b8; white-space: normal; }
    /* Estilo para o valor delta no card de Maior Posição */
    [data-testid="stMetricDelta"] { font-size: 1.2rem; font-weight: 600; color: #a0a0b8 !important; }
</style>
""", unsafe_allow_html=True)


# --- FUNÇÕES DE PROCESSAMENTO DE DADOS ---

def formatar_valor_brl(valor):
    if pd.isna(valor) or valor == '': return "R$ 0,00"
    try:
        valor_float = float(valor)
        return f"R$ {valor_float:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return valor

def limpar_valor_monetario(valor):
    if isinstance(valor, (int, float)): return valor
    if isinstance(valor, str):
        valor_limpo = valor.replace("R$", "").strip().replace(".", "").replace(",", ".")
        try: return float(valor_limpo)
        except (ValueError, TypeError): return 0.0
    return 0.0

def identificar_tipo_opcao(ticker):
    if not isinstance(ticker, str) or len(ticker) < 5: return 'N/D'
    quinta_letra = ticker[4].upper()
    if 'A' <= quinta_letra <= 'L': return 'Call'
    elif 'M' <= quinta_letra <= 'X': return 'Put'
    else: return 'N/D'

# --- FUNÇÕES DE CONEXÃO E MANIPULAÇÃO DO GOOGLE SHEETS ---

def conectar_gsheets():
    """Conecta-se ao Google Sheets usando as credenciais dos Secrets."""
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    client = gspread.authorize(creds)
    return client.open_by_url(st.secrets["private_gsheets_url"])

@st.cache_data(ttl=600, show_spinner="A carregar dados da planilha...")
def carregar_dados_publicos():
    """
    Função otimizada para ler dados de uma Planilha Google.
    """
    try:
        spreadsheet = conectar_gsheets()
        worksheets = spreadsheet.worksheets()
        all_sheets_data = {sheet.title: sheet.get_all_values() for sheet in worksheets}

        sheet_clientes_data = all_sheets_data.get("Clientes", [])
        if not sheet_clientes_data:
             st.error("Aba 'Clientes' não encontrada na Planilha Google.")
             return pd.DataFrame(), {}
        
        df_clientes = pd.DataFrame(sheet_clientes_data[1:], columns=sheet_clientes_data[0])
        df_clientes['Início do Acompanhamento'] = pd.to_datetime(df_clientes['Início do Acompanhamento'], errors='coerce', dayfirst=True)

        nomes_clientes = df_clientes['Nome'].tolist()
        dados_completos_clientes = {}
        meses_pt = ['JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO', 'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO']

        for nome in nomes_clientes:
            if nome in all_sheets_data:
                data = all_sheets_data[nome]
                df_cliente_raw = pd.DataFrame(data).fillna('')
                
                try:
                    start_row_inv = df_cliente_raw[df_cliente_raw[0] == 'CÓDIGO'].index[0]
                    data_inv = df_cliente_raw.iloc[start_row_inv + 1:, :4]
                    df_investimentos = pd.DataFrame(data_inv.values)
                    df_investimentos.columns = ['Código', 'Quantidade', 'Preço Médio', 'Valor Investido']
                    df_investimentos = df_investimentos[df_investimentos['Código'] != ''].dropna(how='all')
                    df_investimentos['Quantidade'] = pd.to_numeric(df_investimentos['Quantidade'], errors='coerce').round().astype('Int64')
                    df_investimentos['Valor Investido'] = df_investimentos['Valor Investido'].apply(limpar_valor_monetario)
                    df_investimentos['Preço Médio'] = df_investimentos['Preço Médio'].apply(limpar_valor_monetario)
                except (IndexError, ValueError):
                    df_investimentos = pd.DataFrame()

                lista_df_opcoes = []
                month_rows_indices = df_cliente_raw[df_cliente_raw.apply(lambda r: any(str(c).upper() in meses_pt for c in r), axis=1)].index.tolist()
                
                for i, start_block_idx in enumerate(month_rows_indices):
                    end_block_idx = month_rows_indices[i + 1] if i + 1 < len(month_rows_indices) else len(df_cliente_raw)
                    mes_atual = next((str(c).capitalize() for c in df_cliente_raw.loc[start_block_idx] if str(c).upper() in meses_pt), None)
                    df_search_area = df_cliente_raw.loc[start_block_idx:end_block_idx-1]
                    header_row_series = df_search_area[df_search_area.apply(lambda r: 'SITUAÇÃO' in r.astype(str).values, axis=1)]
                    
                    if not header_row_series.empty:
                        header_idx = header_row_series.index[0]
                        start_col_op = df_cliente_raw.loc[header_idx][df_cliente_raw.loc[header_idx].astype(str) == 'SITUAÇÃO'].index[0]
                        data_rows = df_cliente_raw.loc[header_idx + 1: end_block_idx - 1]
                        df_temp = data_rows.iloc[:, start_col_op:start_col_op + 7]
                        df_temp.columns = ['Situação', 'Ativo', 'Opção', 'Strike', 'Recomendação', 'Quantidade', 'Preço Executado']
                        df_temp = df_temp[df_temp['Situação'] != ''].dropna(how='all')
                        if not df_temp.empty:
                            df_temp['Mês'] = mes_atual
                            lista_df_opcoes.append(df_temp)

                df_opcoes_final = pd.DataFrame()
                if lista_df_opcoes:
                    df_opcoes_final = pd.concat(lista_df_opcoes, ignore_index=True)
                    df_opcoes_final['Quantidade'] = pd.to_numeric(df_opcoes_final['Quantidade'], errors='coerce').round().astype('Int64')
                    df_opcoes_final['Strike'] = df_opcoes_final['Strike'].apply(limpar_valor_monetario)
                    df_opcoes_final['Preço Executado'] = df_opcoes_final['Preço Executado'].apply(limpar_valor_monetario)
                    df_opcoes_final['Tipo'] = df_opcoes_final['Opção'].apply(identificar_tipo_opcao)

                dados_completos_clientes[nome] = {'investimentos': df_investimentos, 'opcoes': df_opcoes_final}
            else:
                st.warning(f"Aba para o cliente '{nome}' não encontrada.")
                dados_completos_clientes[nome] = {'investimentos': pd.DataFrame(), 'opcoes': pd.DataFrame()}
        
        return df_clientes, dados_completos_clientes

    except Exception as e:
        st.error(f"Não foi possível carregar os dados. Verifique a conexão e as permissões. Erro: {e}")
        return pd.DataFrame(), {}

def adicionar_cliente_na_planilha(dados_cliente, df_carteira):
    """Adiciona um novo cliente e sua carteira à Planilha Google."""
    try:
        spreadsheet = conectar_gsheets()
        
        sheet_clientes = spreadsheet.worksheet("Clientes")
        nova_linha = [
            dados_cliente['nome'], dados_cliente['celular'], dados_cliente['email'],
            dados_cliente['plano'], dados_cliente['inicio'].strftime('%d/%m/%Y')
        ]
        sheet_clientes.append_row(nova_linha, value_input_option='USER_ENTERED')
        
        nova_aba = spreadsheet.add_worksheet(title=dados_cliente['nome'], rows=100, cols=20)
        
        headers_investimentos = [['CÓDIGO', 'QUANTIDADE', 'PM', 'VALOR INVESTIDO']]
        headers_opcoes = [['JUNHO'], [], ['SITUAÇÃO', 'ATIVO', 'OPÇÃO', 'STRIKE', 'RECOMENDAÇÃO', 'QUANTIDADE', 'PREÇO EXECUTADO']]

        nova_aba.update(range_name='A1', values=headers_investimentos)
        
        if not df_carteira.empty:
            df_carteira_lista = df_carteira.astype(str).values.tolist()
            nova_aba.update(range_name='A2', values=df_carteira_lista)
        
        nova_aba.update(range_name='F5', values=headers_opcoes)
        
        return True
    except Exception as e:
        st.error(f"Ocorreu um erro ao guardar os dados: {e}")
        return False

def atualizar_carteira_investimentos(nome_cliente, df_nova_carteira):
    """Atualiza a carteira de investimentos de um cliente existente."""
    try:
        spreadsheet = conectar_gsheets()
        sheet_cliente = spreadsheet.worksheet(nome_cliente)
        
        sheet_cliente.batch_clear(['A2:D100'])
        
        if not df_nova_carteira.empty:
            df_formatada = df_nova_carteira.astype(str)
            sheet_cliente.update(range_name='A2', values=df_formatada.values.tolist(), value_input_option='USER_ENTERED')
        
        return True
    except Exception as e:
        st.error(f"Ocorreu um erro ao atualizar a carteira: {e}")
        return False

def atualizar_carteira_opcoes(nome_cliente, df_nova_carteira_opcoes):
    """Atualiza a carteira de opções completa de um cliente."""
    try:
        spreadsheet = conectar_gsheets()
        sheet_cliente = spreadsheet.worksheet(nome_cliente)
        
        sheet_cliente.batch_clear(['F1:L200'])
        
        if not df_nova_carteira_opcoes.empty:
            grupos_por_mes = df_nova_carteira_opcoes.groupby('Mês')
            linha_atual = 5

            for mes, grupo in grupos_por_mes:
                sheet_cliente.update(range_name=f'F{linha_atual}', values=[[mes.upper()]])
                linha_atual += 2

                cabecalho = [['SITUAÇÃO', 'ATIVO', 'OPÇÃO', 'STRIKE', 'RECOMENDAÇÃO', 'QUANTIDADE', 'PREÇO EXECUTADO']]
                sheet_cliente.update(range_name=f'F{linha_atual}', values=cabecalho)
                linha_atual += 1

                dados_mes = grupo.drop(columns=['Mês', 'Tipo']).astype(str).values.tolist()
                sheet_cliente.update(range_name=f'F{linha_atual}', values=dados_mes, value_input_option='USER_ENTERED')
                linha_atual += len(dados_mes) + 2

        return True
    except Exception as e:
        st.error(f"Ocorreu um erro ao atualizar a carteira de opções: {e}")
        return False

# --- INTERFACE DO DASHBOARD ---
st.title("📈 Dashboard de Acompanhamento de Clientes")
st.markdown("Use o menu na lateral para navegar entre as seções.")

st.sidebar.image("https://i.ibb.co/ymrwQqB1/230x0w.webp", width=100)
st.sidebar.title("Menu de Navegação")
pagina_selecionada = st.sidebar.radio(
    "Selecione uma seção:",
    ("📊 Visão Geral", "💰 Carteira de Investimentos", "📈 Carteira de Opções", "➕ Adicionar Novo Cliente")
)

# --- LÓGICA DE NAVEGAÇÃO ---
if pagina_selecionada == "➕ Adicionar Novo Cliente":
    st.header("➕ Adicionar Novo Cliente")
    df_clientes_geral, _ = carregar_dados_publicos()
    
    with st.form(key="novo_cliente_form"):
        st.subheader("Dados Pessoais")
        col1, col2 = st.columns(2)
        with col1:
            nome_cliente = st.text_input("Nome Completo*")
            email_cliente = st.text_input("Email")
        with col2:
            celular_cliente = st.text_input("Celular")
            plano_cliente = st.selectbox("Plano*", ("Eleva", "Alavanca"))
        
        inicio_acompanhamento = st.date_input("Início do Acompanhamento*", datetime.now(), format="DD/MM/YYYY")

        st.subheader("Carteira de Investimentos Inicial")
        df_carteira_vazia = pd.DataFrame(columns=['Código', 'Quantidade', 'Preço Médio', 'Valor Investido'])
        carteira_editada = st.data_editor(df_carteira_vazia, num_rows="dynamic", use_container_width=True)
        
        submit_button = st.form_submit_button(label="Salvar Novo Cliente")

    if submit_button:
        emails_existentes = df_clientes_geral['Email'].str.strip().str.lower().tolist()
        if not nome_cliente:
            st.warning("O campo 'Nome Completo' é obrigatório.")
        elif email_cliente and email_cliente.strip().lower() in emails_existentes:
            st.error("Este email já está cadastrado. Por favor, utilize outro.")
        else:
            with st.spinner("A guardar novo cliente na planilha..."):
                dados_novo_cliente = {"nome": nome_cliente, "celular": celular_cliente, "email": email_cliente, "plano": plano_cliente, "inicio": inicio_acompanhamento}
                carteira_final = carteira_editada.dropna(how='all').copy()
                sucesso = adicionar_cliente_na_planilha(dados_novo_cliente, carteira_final)
                if sucesso:
                    st.success(f"Cliente '{nome_cliente}' adicionado com sucesso!")
                    st.balloons()
                    st.cache_data.clear()

else:
    df_clientes, dados_carteiras = carregar_dados_publicos()
    if df_clientes.empty:
        st.warning("Nenhum dado de cliente para exibir.")
        st.stop()
    
    if pagina_selecionada == "📊 Visão Geral":
        st.header("📊 Visão Geral dos Clientes")
        patrimonio_total = sum(dados_carteiras[nome]['investimentos']['Valor Investido'].sum() for nome in df_clientes['Nome'] if nome in dados_carteiras and not dados_carteiras[nome]['investimentos'].empty)
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

    elif pagina_selecionada == "💰 Carteira de Investimentos":
        st.header("💰 Análise da Carteira de Investimentos")
        cliente_selecionado = st.sidebar.selectbox("Selecione um Cliente", options=df_clientes['Nome'].unique())
        st.sidebar.caption("Clique na caixa e digite para pesquisar.")
        if cliente_selecionado:
            df_invest = dados_carteiras.get(cliente_selecionado, {}).get('investimentos', pd.DataFrame())
            
            patrimonio_cliente = df_invest['Valor Investido'].sum() if not df_invest.empty else 0
            num_ativos = len(df_invest)
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Patrimônio Total do Cliente", formatar_valor_brl(patrimonio_cliente))
            col2.metric("Número de Ativos", num_ativos)
            
            if not df_invest.empty:
                maior_posicao = df_invest.loc[df_invest['Valor Investido'].idxmax()]
                col3.metric(label="Maior Posição", value=maior_posicao['Código'], delta=formatar_valor_brl(maior_posicao['Valor Investido']), delta_color="off")
            else:
                col3.metric(label="Maior Posição", value="N/A")
            
            st.markdown("---")
            col_graf1, col_graf2 = st.columns([0.4, 0.6])
            with col_graf1:
                if not df_invest.empty:
                    fig_composicao = px.pie(df_invest, names='Código', values='Valor Investido', title=f'Composição da Carteira de {cliente_selecionado}', hole=0.4)
                    st.plotly_chart(fig_composicao, use_container_width=True)
            with col_graf2:
                if not df_invest.empty:
                    fig_barras = px.bar(df_invest.sort_values('Valor Investido', ascending=False), x='Código', y='Valor Investido', title=f'Valor Investido por Ativo', text_auto='.2s')
                    st.plotly_chart(fig_barras, use_container_width=True)

            st.subheader("Tabela Detalhada e Edição da Carteira")
            with st.form(key="edicao_carteira_inline"):
                with st.expander("Filtros da Tabela", expanded=False):
                    codigos_disponiveis = sorted(df_invest['Código'].dropna().unique())
                    codigos_selecionados = st.multiselect("Filtrar por Código:", options=codigos_disponiveis, default=codigos_disponiveis, key=f"filtro_cod_{cliente_selecionado}")
                
                df_invest_filtrada = df_invest[df_invest['Código'].isin(codigos_selecionados)]
                
                # O editor agora mostra a carteira completa para edição
                carteira_para_editar = st.data_editor(df_invest, num_rows="dynamic", use_container_width=True, key=f"editor_{cliente_selecionado}")
                
                submitted = st.form_submit_button("Salvar Alterações")
                if submitted:
                    with st.spinner("A atualizar carteira..."):
                        sucesso = atualizar_carteira_investimentos(cliente_selecionado, carteira_para_editar)
                        if sucesso:
                            st.success("Carteira atualizada com sucesso!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Falha ao atualizar a carteira.")

    elif pagina_selecionada == "📈 Carteira de Opções":
        st.header("📈 Análise da Carteira de Opções")
        cliente_selecionado_op = st.sidebar.selectbox("Selecione um Cliente", options=df_clientes['Nome'].unique(), key="cliente_opcoes")
        st.sidebar.caption("Clique na caixa e digite para pesquisar.")
        if cliente_selecionado_op:
            df_opcoes = dados_carteiras.get(cliente_selecionado_op, {}).get('opcoes', pd.DataFrame())
            
            meses_disponiveis = sorted(df_opcoes['Mês'].dropna().unique()) if not df_opcoes.empty else []
            if meses_disponiveis:
                mes_filtrado = st.sidebar.selectbox("Filtrar por Mês", options=meses_disponiveis)
                df_mes_filtrado = df_opcoes[df_opcoes['Mês'] == mes_filtrado]
                col_filtro1, col_filtro2 = st.columns(2)
                with col_filtro1:
                    ativos_disponiveis = sorted(df_mes_filtrado['Ativo'].dropna().unique())
                    ativo_filtrado = st.multiselect("Filtrar por Ativo", options=ativos_disponiveis, default=ativos_disponiveis)
                with col_filtro2:
                    situacoes_disponiveis = sorted(df_mes_filtrado['Situação'].dropna().unique())
                    situacao_filtrada = st.multiselect("Filtrar por Situação", options=situacoes_disponiveis, default=situacoes_disponiveis)
                df_filtrada = df_mes_filtrado[df_mes_filtrado['Ativo'].isin(ativo_filtrado) & df_mes_filtrado['Situação'].isin(situacao_filtrada)]
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
            
            st.subheader("Tabela Detalhada e Edição da Carteira de Opções")
            with st.form(key="edicao_opcoes_inline"):
                with st.expander("Filtros da Tabela", expanded=False):
                    col_f1, col_f2, col_f3 = st.columns(3)
                    
                    opcoes_display_filtrada = df_opcoes.copy()
                    
                    with col_f1:
                        meses_filtro = sorted(df_opcoes['Mês'].dropna().unique())
                        meses_selecionados = st.multiselect("Mês:", options=meses_filtro, default=meses_filtro, key=f"filtro_mes_{cliente_selecionado_op}")
                        opcoes_display_filtrada = opcoes_display_filtrada[opcoes_display_filtrada['Mês'].isin(meses_selecionados)]

                    with col_f2:
                        ativos_filtro = sorted(opcoes_display_filtrada['Ativo'].dropna().unique())
                        ativos_selecionados = st.multiselect("Ativo:", options=ativos_filtro, default=ativos_filtro, key=f"filtro_ativo_{cliente_selecionado_op}")
                        opcoes_display_filtrada = opcoes_display_filtrada[opcoes_display_filtrada['Ativo'].isin(ativos_selecionados)]

                    with col_f3:
                        situacoes_filtro = sorted(opcoes_display_filtrada['Situação'].dropna().unique())
                        situacoes_selecionadas = st.multiselect("Situação:", options=situacoes_filtro, default=situacoes_filtro, key=f"filtro_sit_{cliente_selecionado_op}")
                        opcoes_display_filtrada = opcoes_display_filtrada[opcoes_display_filtrada['Situação'].isin(situacoes_selecionadas)]

                colunas_edicao = ['Situação', 'Ativo', 'Opção', 'Strike', 'Recomendação', 'Quantidade', 'Preço Executado', 'Mês']
                if df_opcoes.empty:
                    carteira_opcoes_para_editar = st.data_editor(pd.DataFrame(columns=colunas_edicao), num_rows="dynamic", use_container_width=True, key=f"editor_opcoes_{cliente_selecionado_op}")
                else:
                    # O editor mostra a carteira completa, mas a visualização acima pode ser filtrada
                    carteira_opcoes_para_editar = st.data_editor(df_opcoes[colunas_edicao], num_rows="dynamic", use_container_width=True, key=f"editor_opcoes_{cliente_selecionado_op}")
                
                submitted_opcoes = st.form_submit_button("Salvar Alterações na Carteira de Opções")
                if submitted_opcoes:
                    with st.spinner("A atualizar carteira de opções..."):
                        carteira_opcoes_para_editar['Tipo'] = carteira_opcoes_para_editar['Opção'].apply(identificar_tipo_opcao)
                        sucesso = atualizar_carteira_opcoes(cliente_selecionado_op, carteira_opcoes_para_editar)
                        if sucesso:
                            st.success("Carteira de opções atualizada com sucesso!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Falha ao atualizar a carteira de opções.")

st.sidebar.markdown("---")
st.sidebar.info("Dashboard desenvolvido para gestão de carteiras. v2.9")
