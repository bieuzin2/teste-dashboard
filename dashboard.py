import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta
import re
from streamlit_calendar import calendar # Nova importação

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(layout="wide", page_title="Dashboard de Clientes")

# --- ESTILOS CSS CUSTOMIZADOS ---
st.markdown("""
<style>
    /* Aplica a cor verde escura aos títulos */
    h1, h2, h3 {
        color: #075025 !important;
    }

    /* Estilo dos cards de métrica com nova paleta */
    [data-testid="stMetric"] {
        background-color: #075025; /* Verde escuro no fundo */
        border: 1px solid #0C773C; /* Verde mais claro na borda */
        padding: 15px;
        border-radius: 10px;
        color: white;
    }
    /* Nova cor para o valor da métrica (dourado) */
    [data-testid="stMetricValue"] { font-size: 2.2rem; font-weight: 600; color: #BE9D5B; }
    [data-testid="stMetricLabel"] { font-size: 1rem; color: #f0f2f6; opacity: 0.8; white-space: normal; }
    [data-testid="stMetricDelta"] { font-size: 1.2rem; font-weight: 600; color: #f0f2f6 !important; opacity: 0.8;}
    
    /* Ajusta a altura do calendário */
    .fc-view-harness {
        height: 350px !important;
    }
    /* Esconde o título do evento (ponto) */
    .fc-event-title {
        display: none;
    }
    /* Transforma o evento em um ponto centralizado */
    .fc-daygrid-event {
        border-radius: 50%;
        width: 8px;
        height: 8px;
        margin: 5px auto 0 auto; /* Centraliza o ponto */
    }
    /* Pinta o fundo do dia inteiro e o torna clicável */
    .fc-daygrid-day-events {
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        opacity: 0.3; /* Deixa a cor mais suave */
        cursor: pointer;
    }
    /* Remove o ponto do evento para não ficar redundante */
    a.fc-daygrid-event.fc-daygrid-dot-event.fc-event.fc-event-start.fc-event-end.fc-event-today {
        display: none;
    }
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

def calcular_data_vencimento(row):
    """
    Calcula a data de vencimento correta para opções MENSAIS e SEMANAIS.
    - Mensais: 3ª sexta-feira do mês.
    - Semanais (com W1, W2, W4, W5 no código): 1ª, 2ª, 4ª ou 5ª sexta-feira.
    """
    mes_str = row['Mês']
    ticker = row['Opção']

    if not isinstance(mes_str, str) or not isinstance(ticker, str):
        return None
    
    mes_map = {
        'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6,
        'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 'Dezembro': 12
    }
    
    num_mes = mes_map.get(mes_str.capitalize())
    if not num_mes: return None

    ano = datetime.now().year
    primeiro_dia_mes = date(ano, num_mes, 1)
    
    dias_para_sexta = (4 - primeiro_dia_mes.weekday() + 7) % 7
    primeira_sexta = primeiro_dia_mes + timedelta(days=dias_para_sexta)
    
    match = re.search(r'W([1245])', ticker.upper())
    
    if match:
        semana = int(match.group(1))
        vencimento = None
        if semana == 1: vencimento = primeira_sexta
        elif semana == 2: vencimento = primeira_sexta + timedelta(days=7)
        elif semana == 4: vencimento = primeira_sexta + timedelta(days=21)
        elif semana == 5: vencimento = primeira_sexta + timedelta(days=28)
        
        if vencimento and vencimento.month == num_mes:
            return vencimento
        else:
            return None
    else:
        terceira_sexta = primeira_sexta + timedelta(days=14)
        return terceira_sexta

# --- FUNÇÕES DE CONEXÃO E MANIPULAÇÃO DO GOOGLE SHEETS ---

def conectar_gsheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    client = gspread.authorize(creds)
    return client.open_by_url(st.secrets["private_gsheets_url"])

@st.cache_data(ttl=600, show_spinner="A carregar dados da planilha...")
def carregar_dados_publicos():
    try:
        spreadsheet = conectar_gsheets()
        worksheets = spreadsheet.worksheets()
        all_sheets_data = {sheet.title: sheet.get_all_values() for sheet in worksheets}

        sheet_clientes_data = all_sheets_data.get("Clientes", [])
        if not sheet_clientes_data:
             st.error("Aba 'Clientes' não encontrada na Planilha Google.")
             return pd.DataFrame(), {}, pd.DataFrame()
        
        df_clientes = pd.DataFrame(sheet_clientes_data[1:], columns=sheet_clientes_data[0])
        df_clientes['Início do Acompanhamento'] = pd.to_datetime(df_clientes['Início do Acompanhamento'], errors='coerce', dayfirst=True)

        nomes_clientes = df_clientes['Nome'].tolist()
        dados_completos_clientes = {}
        lista_opcoes_geral = []
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
                    
                    df_opcoes_cliente = df_opcoes_final.copy()
                    df_opcoes_cliente['Cliente'] = nome
                    lista_opcoes_geral.append(df_opcoes_cliente)

                dados_completos_clientes[nome] = {'investimentos': df_investimentos, 'opcoes': df_opcoes_final}
            else:
                st.warning(f"Aba para o cliente '{nome}' não encontrada.")
                dados_completos_clientes[nome] = {'investimentos': pd.DataFrame(), 'opcoes': pd.DataFrame()}
        
        df_todas_opcoes = pd.DataFrame()
        if lista_opcoes_geral:
            df_todas_opcoes = pd.concat(lista_opcoes_geral, ignore_index=True)
            df_todas_opcoes['Data de Vencimento'] = df_todas_opcoes.apply(calcular_data_vencimento, axis=1)
            df_todas_opcoes.dropna(subset=['Data de Vencimento'], inplace=True)
            df_todas_opcoes['Data de Vencimento'] = pd.to_datetime(df_todas_opcoes['Data de Vencimento'])

        return df_clientes, dados_completos_clientes, df_todas_opcoes

    except Exception as e:
        st.error(f"Não foi possível carregar os dados. Verifique a conexão e as permissões. Erro: {e}")
        return pd.DataFrame(), {}, pd.DataFrame()

def adicionar_cliente_na_planilha(dados_cliente, df_carteira):
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
        mes_atual_nome = datetime.now().strftime('%B').upper()
        headers_opcoes = [[mes_atual_nome], [], ['SITUAÇÃO', 'ATIVO', 'OPÇÃO', 'STRIKE', 'RECOMENDAÇÃO', 'QUANTIDADE', 'PREÇO EXECUTADO']]

        nova_aba.update(range_name='A1', values=headers_investimentos)
        
        if not df_carteira.empty:
            df_para_salvar = df_carteira.copy()
            colunas_monetarias = ['Preço Médio', 'Valor Investido']
            for col in colunas_monetarias:
                if col in df_para_salvar.columns:
                    df_para_salvar[col] = df_para_salvar[col].apply(
                        lambda x: f'{x:.2f}'.replace('.', ',') if pd.notna(x) and isinstance(x, (int, float)) else x
                    )
            nova_aba.update(range_name='A2', values=df_para_salvar.astype(str).values.tolist())
        
        nova_aba.update(range_name='F5', values=headers_opcoes)
        
        return True
    except Exception as e:
        st.error(f"Ocorreu um erro ao guardar os dados: {e}")
        return False

def atualizar_carteira_investimentos(nome_cliente, df_nova_carteira):
    try:
        spreadsheet = conectar_gsheets()
        sheet_cliente = spreadsheet.worksheet(nome_cliente)
        
        sheet_cliente.batch_clear(['A2:D100'])
        
        if not df_nova_carteira.empty:
            df_para_salvar = df_nova_carteira.copy()
            colunas_monetarias = ['Preço Médio', 'Valor Investido']
            for col in colunas_monetarias:
                if col in df_para_salvar.columns:
                    df_para_salvar[col] = df_para_salvar[col].apply(
                        lambda x: f'{x:.2f}'.replace('.', ',') if pd.notna(x) and isinstance(x, (int, float)) else x
                    )
            
            sheet_cliente.update(range_name='A2', values=df_para_salvar.astype(str).values.tolist(), value_input_option='USER_ENTERED')
        
        return True
    except Exception as e:
        st.error(f"Ocorreu um erro ao atualizar a carteira: {e}")
        return False

def atualizar_carteira_opcoes(nome_cliente, df_nova_carteira_opcoes):
    try:
        spreadsheet = conectar_gsheets()
        sheet_cliente = spreadsheet.worksheet(nome_cliente)
        
        sheet_cliente.batch_clear(['F1:L200'])
        
        if not df_nova_carteira_opcoes.empty:
            mes_map_inv = {
                1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho',
                7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
            }
            df_nova_carteira_opcoes['num_mes'] = df_nova_carteira_opcoes['Mês'].str.capitalize().map({v: k for k, v in mes_map_inv.items()})
            grupos_por_mes = sorted(df_nova_carteira_opcoes.groupby('Mês'), key=lambda x: x[1]['num_mes'].iloc[0])

            linha_atual = 5

            for mes, grupo in grupos_por_mes:
                sheet_cliente.update(range_name=f'F{linha_atual}', values=[[mes.upper()]])
                linha_atual += 2

                cabecalho = [['SITUAÇÃO', 'ATIVO', 'OPÇÃO', 'STRIKE', 'RECOMENDAÇÃO', 'QUANTIDADE', 'PREÇO EXECUTADO']]
                sheet_cliente.update(range_name=f'F{linha_atual}', values=cabecalho)
                linha_atual += 1
                
                grupo_para_salvar = grupo.copy()
                colunas_monetarias = ['Strike', 'Preço Executado']
                for col in colunas_monetarias:
                    if col in grupo_para_salvar.columns:
                         grupo_para_salvar[col] = grupo_para_salvar[col].apply(
                            lambda x: f'{x:.2f}'.replace('.', ',') if pd.notna(x) and isinstance(x, (int, float)) else x
                        )

                dados_mes = grupo_para_salvar.drop(columns=['Mês', 'Tipo', 'num_mes']).astype(str).values.tolist()
                sheet_cliente.update(range_name=f'F{linha_atual}', values=dados_mes, value_input_option='USER_ENTERED')
                linha_atual += len(dados_mes) + 2

        return True
    except Exception as e:
        st.error(f"Ocorreu um erro ao atualizar a carteira de opções: {e}")
        return False

# --- INTERFACE DO DASHBOARD ---
st.title("Dashboard de Acompanhamento de Clientes")
st.markdown("Use o menu na lateral para navegar entre as seções.")

st.sidebar.image("https://i.ibb.co/ymrwQqB1/230x0w.webp", width=100)
st.sidebar.title("Menu de Navegação")
pagina_selecionada = st.sidebar.radio(
    "Selecione uma seção:",
    ("📊 Visão Geral", "💰 Carteira de Investimentos", "📈 Carteira de Opções", "📅 Calendário de Vencimentos", "➕ Adicionar Novo Cliente")
)

# --- LÓGICA DE NAVEGAÇÃO ---
if pagina_selecionada == "➕ Adicionar Novo Cliente":
    st.header("Adicionar Novo Cliente")
    df_clientes_geral, _, _ = carregar_dados_publicos()
    
    with st.form(key="novo_cliente_form"):
        st.subheader("Dados Pessoais")
        col1, col2 = st.columns(2)
        with col1:
            nome_cliente = st.text_input("Nome Completo*")
            email_cliente = st.text_input("Email")
        with col2:
            celular_cliente = st.text_input("Celular (com DDD, ex: 21987654321)")
            plano_cliente = st.selectbox("Plano*", ("Eleva", "Alavanca"))
        
        inicio_acompanhamento = st.date_input("Início do Acompanhamento*", datetime.now(), format="DD/MM/YYYY")

        st.subheader("Carteira de Investimentos Inicial")
        df_carteira_vazia = pd.DataFrame(columns=['Código', 'Quantidade', 'Preço Médio', 'Valor Investido'])
        carteira_editada = st.data_editor(
            df_carteira_vazia, 
            num_rows="dynamic", 
            use_container_width=True,
            column_config={
                "Preço Médio": st.column_config.NumberColumn("Preço Médio", format="R$ %.2f"),
                "Valor Investido": st.column_config.NumberColumn("Valor Investido", format="R$ %.2f")
            }
        )
        
        submit_button = st.form_submit_button(label="Salvar Novo Cliente")

    if submit_button:
        emails_existentes = df_clientes_geral['Email'].str.strip().str.lower().tolist() if 'Email' in df_clientes_geral.columns else []
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
    df_clientes, dados_carteiras, df_todas_opcoes = carregar_dados_publicos()
    if df_clientes.empty:
        st.warning("Nenhum dado de cliente para exibir.")
        st.stop()
    
    if pagina_selecionada == "📊 Visão Geral":
        st.header("Visão Geral dos Clientes")
        patrimonio_total = sum(dados_carteiras[nome]['investimentos']['Valor Investido'].sum() for nome in df_clientes['Nome'] if nome in dados_carteiras and not dados_carteiras[nome]['investimentos'].empty)
        total_clientes = len(df_clientes)
        col1, col2 = st.columns(2)
        col1.metric(label="Total de Clientes", value=total_clientes)
        col2.metric(label="Patrimônio Total Investido", value=formatar_valor_brl(patrimonio_total))
        st.markdown("---")
        col_graf1, col_graf2 = st.columns(2)
        with col_graf1:
            # Gráfico de Pizza com nova paleta
            fig_plano = px.pie(df_clientes, names='Plano', title='Distribuição de Clientes por Plano', hole=0.4, 
                               color_discrete_sequence=['#075025', '#0C773C', '#BE9D5B'])
            st.plotly_chart(fig_plano, use_container_width=True)
        with col_graf2:
            df_clientes_por_data = df_clientes.dropna(subset=['Início do Acompanhamento']).set_index('Início do Acompanhamento').resample('ME').size().reset_index(name='Novos Clientes')
            # Gráfico de Linha com nova paleta
            fig_evolucao = px.line(df_clientes_por_data, x='Início do Acompanhamento', y='Novos Clientes', title='Evolução de Inícios de Acompanhamento', markers=True)
            fig_evolucao.update_traces(line_color='#0C773C', marker_color='#BE9D5B')
            st.plotly_chart(fig_evolucao, use_container_width=True)
        st.subheader("Lista de Clientes")
        df_clientes_display = df_clientes.copy()
        df_clientes_display.index = range(1, len(df_clientes_display) + 1)
        st.dataframe(df_clientes_display.style.format({"Início do Acompanhamento": lambda x: x.strftime('%d/%m/%Y') if pd.notna(x) else ''}), use_container_width=True)

    elif pagina_selecionada == "💰 Carteira de Investimentos":
        st.header("Análise da Carteira de Investimentos")
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
            st.subheader("Tabela Detalhada e Edição da Carteira")
            with st.form(key="edicao_carteira_inline"):
                carteira_para_editar = st.data_editor(
                    df_invest, 
                    num_rows="dynamic", 
                    use_container_width=True, 
                    key=f"editor_{cliente_selecionado}",
                    column_config={
                        "Preço Médio": st.column_config.NumberColumn("Preço Médio", format="R$ %.2f"),
                        "Valor Investido": st.column_config.NumberColumn("Valor Investido", format="R$ %.2f")
                    }
                )
                
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
        st.header("Análise da Carteira de Opções")
        cliente_selecionado_op = st.sidebar.selectbox("Selecione um Cliente", options=df_clientes['Nome'].unique(), key="cliente_opcoes")
        st.sidebar.caption("Clique na caixa e digite para pesquisar.")
        if cliente_selecionado_op:
            df_opcoes = dados_carteiras.get(cliente_selecionado_op, {}).get('opcoes', pd.DataFrame())
            
            st.subheader("Tabela Detalhada e Edição da Carteira de Opções")
            with st.form(key="edicao_opcoes_inline"):
                colunas_edicao = ['Situação', 'Ativo', 'Opção', 'Strike', 'Recomendação', 'Quantidade', 'Preço Executado', 'Mês']
                
                df_para_editar = df_opcoes[colunas_edicao] if not df_opcoes.empty else pd.DataFrame(columns=colunas_edicao)

                carteira_opcoes_para_editar = st.data_editor(
                    df_para_editar, 
                    num_rows="dynamic", 
                    use_container_width=True, 
                    key=f"editor_opcoes_{cliente_selecionado_op}",
                    column_config={
                        "Strike": st.column_config.NumberColumn("Strike", format="R$ %.2f"),
                        "Preço Executado": st.column_config.NumberColumn("Preço Executado", format="R$ %.2f")
                    }
                )
                
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
    
    # --- PÁGINA DE CALENDÁRIO COMPLETAMENTE REFEITA ---
    elif pagina_selecionada == "📅 Calendário de Vencimentos":
        st.header("Calendário Interativo de Vencimentos")

        if df_todas_opcoes.empty:
            st.info("Não há operações com opções cadastradas para exibir no calendário.")
            st.stop()

        # Filtra apenas vencimentos futuros
        hoje = pd.to_datetime('today').normalize()
        df_futuras = df_todas_opcoes[df_todas_opcoes['Data de Vencimento'] >= hoje].copy()

        if df_futuras.empty:
            st.info("Não há vencimentos futuros para exibir.")
            st.stop()

        # --- NOVO LAYOUT DE COLUNAS ---
        col_cal, col_list = st.columns([1, 2])

        with col_cal:
            st.subheader("Navegação")
            # --- PREPARA OS EVENTOS PARA O CALENDÁRIO ---
            df_futuras['Dias para Vencer'] = (df_futuras['Data de Vencimento'] - hoje).dt.days
            
            def definir_cor(dias):
                if dias <= 7: return "#c0392b"  # Vermelho
                if dias <= 15: return "#f1c40f" # Amarelo
                return "#075025" # Verde Escuro
            
            df_futuras['Cor'] = df_futuras['Dias para Vencer'].apply(definir_cor)

            calendar_events = []
            # Agrupa para mostrar apenas um ponto por dia
            for venc_date, group in df_futuras.groupby('Data de Vencimento'):
                event = {
                    "title": "●", # Título como um ponto para garantir visibilidade
                    "color": group['Cor'].iloc[0], # Pega a cor do alerta mais próximo
                    "start": venc_date.strftime("%Y-%m-%d"),
                    "end": venc_date.strftime("%Y-%m-%d"),
                    "allDay": True,
                    "display": "background", # Pinta o fundo do dia
                }
                calendar_events.append(event)
            
            # --- CONFIGURAÇÕES DO CALENDÁRIO ---
            calendar_options = {
                "headerToolbar": {
                    "left": "today prev,next", "center": "title", "right": "",
                },
                "initialView": "dayGridMonth", "locale": "pt-br",
                "navLinks": False, "selectable": True,
            }

            # Renderiza o calendário
            state = calendar(
                events=calendar_events, options=calendar_options,
                key="calendar_vencimentos"
            )

            # --- LÓGICA DE ATUALIZAÇÃO DO ESTADO ---
            if state.get("dateClick"):
                data_clicada_str = state["dateClick"]["date"].split("T")[0]
                st.session_state.selected_date = datetime.strptime(data_clicada_str, "%Y-%m-%d").date()
            
        with col_list:
            # --- FILTROS EXPANSÍVEIS ---
            with st.expander("🔍 Mostrar/Ocultar Filtros"):
                c1, c2, c3 = st.columns(3)
                with c1:
                    clientes_disponiveis = sorted(df_futuras['Cliente'].unique())
                    clientes_selecionados = st.multiselect("Cliente:", options=clientes_disponiveis, default=clientes_disponiveis)
                with c2:
                    opcoes_disponiveis = sorted(df_futuras['Opção'].unique())
                    opcoes_selecionadas = st.multiselect("Opção:", options=opcoes_disponiveis, default=opcoes_disponiveis)
                with c3:
                    datas_disponiveis = sorted(df_futuras['Data de Vencimento'].dt.date.unique())
                    datas_selecionadas = st.multiselect("Data:", options=datas_disponiveis, default=datas_disponiveis)

            # Aplica filtros
            df_filtrada = df_futuras[
                df_futuras['Cliente'].isin(clientes_selecionados) &
                df_futuras['Opção'].isin(opcoes_selecionadas) &
                df_futuras['Data de Vencimento'].dt.date.isin(datas_selecionadas)
            ]

            if df_filtrada.empty:
                st.warning("Nenhuma operação encontrada com os filtros selecionados.")
            else:
                # Helper para criar URL do WhatsApp
                def criar_url_wpp(celular):
                    if pd.notna(celular) and str(celular).strip():
                        celular_limpo = ''.join(filter(str.isdigit, str(celular)))
                        return f"https://wa.me/{celular_limpo}"
                    return None

                # Se uma data foi clicada, mostra os detalhes daquele dia
                if 'selected_date' in st.session_state and st.session_state.selected_date:
                    data_selecionada = st.session_state.selected_date
                    
                    col_btn1, col_btn2 = st.columns([2, 1])
                    with col_btn2:
                        if st.button("⬅️ Ver todos os vencimentos"):
                            st.session_state.selected_date = None
                            st.rerun()

                    vencimentos_do_dia = df_filtrada[df_filtrada['Data de Vencimento'].dt.date == data_selecionada]

                    if not vencimentos_do_dia.empty:
                        with col_btn1:
                            st.subheader(f"Vencimentos para {data_selecionada.strftime('%d/%m/%Y')}")
                        
                        vencimentos_com_contato = pd.merge(vencimentos_do_dia, df_clientes[['Nome', 'Celular']], left_on='Cliente', right_on='Nome', how='left')
                        clientes_do_dia = vencimentos_com_contato.groupby('Cliente')

                        for nome_cliente, df_cliente in clientes_do_dia:
                            celular = df_cliente['Celular'].iloc[0]
                            url_wpp = criar_url_wpp(celular)

                            c1, c2 = st.columns([3, 1])
                            with c1:
                                st.markdown(f"**Cliente:** {nome_cliente}")
                            with c2:
                                if url_wpp:
                                    st.link_button("Contatar", url=url_wpp)
                            
                            st.dataframe(
                                df_cliente[['Opção', 'Ativo', 'Strike', 'Quantidade', 'Tipo']],
                                hide_index=True, use_container_width=True,
                                column_config={"Strike": st.column_config.NumberColumn("Strike", format="R$ %.2f")}
                            )
                            st.divider()
                    else:
                        st.info(f"Nenhum vencimento para {data_selecionada.strftime('%d/%m/%Y')} com os filtros atuais.")
                
                # Se nenhuma data foi clicada, mostra a lista geral
                else:
                    st.subheader("Próximos Vencimentos")
                    df_display = pd.merge(df_filtrada, df_clientes[['Nome', 'Celular']], left_on='Cliente', right_on='Nome', how='left')
                    df_display['Ação'] = df_display['Celular'].apply(criar_url_wpp)
                    
                    df_display = df_display.sort_values(by="Data de Vencimento").rename(columns={
                        'Data de Vencimento': 'Vencimento',
                    })

                    colunas_tabela = ['Vencimento', 'Cliente', 'Ativo', 'Opção', 'Strike', 'Quantidade', 'Tipo', 'Ação']
                    st.dataframe(
                        df_display[colunas_tabela],
                        column_config={
                            "Vencimento": st.column_config.DateColumn("Vencimento", format="DD/MM/YYYY"),
                            "Strike": st.column_config.NumberColumn("Strike", format="R$ %.2f"),
                            "Ação": st.column_config.LinkColumn("Ação", display_text="Contatar")
                        },
                        use_container_width=True,
                        hide_index=True
                    )

st.sidebar.markdown("---")
st.sidebar.info("Dashboard desenvolvido para gestão de carteiras. v2.0")
