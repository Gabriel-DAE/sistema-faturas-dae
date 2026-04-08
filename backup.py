import psycopg
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import os
from datetime import datetime

# Configurações via Variáveis de Ambiente (GitHub Secrets)
DB_URL = os.getenv("DATABASE_URL")
EMAIL_USER = os.getenv("EMAIL_SENDER")      # Seu e-mail (ex: Gmail)
EMAIL_PASS = os.getenv("EMAIL_PASSWORD")    # Senha de app do e-mail
EMAIL_DEST = os.getenv("EMAIL_RECEIVER")    # E-mail que receberá o backup

def gerar_backup():
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    nome_arquivo = f"backup_faturas_dae_{data_hoje}.xlsx"
    
    try:
        # 1. Conectar e Extrair Dados
        with psycopg.connect(DB_URL) as conn:
            df_faturas = pd.read_sql_query("SELECT * FROM faturas_cpfl", conn)
            df_ucs = pd.read_sql_query("SELECT * FROM cadastro_uc", conn)
            df_tarifas = pd.read_sql_query("SELECT * FROM parametros_faturamento", conn)

        # 2. Criar Excel com múltiplas abas
        with pd.ExcelWriter(nome_arquivo, engine='openpyxl') as writer:
            df_faturas.to_excel(writer, index=False, sheet_name='Faturas')
            df_ucs.to_excel(writer, index=False, sheet_name='UCs')
            df_tarifas.to_excel(writer, index=False, sheet_name='Tarifas')

        # 3. Enviar E-mail
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = EMAIL_DEST
        msg['Subject'] = f"📦 Backup Automático - Sistema de Faturas DAE ({data_hoje})"
        msg.attach(MIMEText("Segue em anexo o backup semanal das tabelas do banco de dados PostgreSQL (Supabase).", 'plain'))

        with open(nome_arquivo, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f"attachment; filename= {nome_arquivo}")
            msg.attach(part)

        # Configuração do servidor SMTP (Exemplo para Gmail)
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
        server.quit()
        
        print(f"✅ Backup enviado com sucesso: {nome_arquivo}")
        os.remove(nome_arquivo) # Limpa o ficheiro temporário

    except Exception as e:
        print(f"❌ Erro no backup: {e}")

if __name__ == "__main__":
    gerar_backup()
