from __future__ import annotations

import os
import re
import smtplib
import json
import logging
from datetime import datetime, timedelta, date
from email.message import EmailMessage
from email.utils import make_msgid, formatdate
from urllib.parse import quote

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# =============================================================================
# Configurações principais
# =============================================================================

DAG_ID = "teste_nextcloud"
DESCRIPTION = "Executa consulta no Impala, gera Excel, publica no Nextcloud (link fixo) e envia e-mail."
SCHEDULE = None  # execute manualmente; mude para cron/@daily se quiser
START_DATE = datetime(2025, 8, 1)

CONN_ID = "impala_via_haproxy"

# Arquivo local (em volume compartilhado do Airflow)
OUTPUT_DIR = "/opt/airflow/dags/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILENAME = "impala_result.xlsx"  # nome fixo (também no Nextcloud)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

# Limite de linhas para manter anexo leve
MAX_ROWS = 60000

# =============================================================================
# E-mail / relay
# =============================================================================

MAIL_SERVER = "mail-relay"
MAIL_PORT = 25

# Envelope sender real (relay)
MAIL_FROM_ENVELOPE = "cgdai.noreply@spu.gov.br"
# Cabeçalho visível
MAIL_FROM_HEADER = "SPU – Relatórios <noreply@spu.gov.br>"
MAIL_REPLY_TO = "noreply@spu.gov.br"

MAIL_TO = [
    "leonardo.mota@gestao.gov.br",
    "flavio-s.araujo@gestao.gov.br",
]
MAIL_SUBJECT = "Relatório Acervo (Memória SPU)"
MAIL_EHLO_HOST = "spu.gov.br"  # host no EHLO

# =============================================================================
# Nextcloud (Airflow Variables)
# =============================================================================

NC_BASE = Variable.get("NEXTCLOUD_BASE_URL")                  # ex: https://spudrive.duckdns.org
NC_USER = Variable.get("NEXTCLOUD_USERNAME")                  # ex: cgdaidrive
NC_PASS = Variable.get("NEXTCLOUD_APP_PASSWORD")              # senha de app
NC_DIR  = Variable.get("NEXTCLOUD_GEOJSON_DIR")               # ex: CGDAI/Relatorios/airflow
VERIFY_SSL = Variable.get("NEXTCLOUD_VERIFY_SSL", default_var="true").lower() == "true"

# Compartilhamento público do arquivo
NC_SHARE_PASSWORD = Variable.get("NEXTCLOUD_SHARE_PASSWORD", default_var=None)  # opcional
NC_SHARE_EXPIRE_DAYS = int(Variable.get("NEXTCLOUD_SHARE_EXPIRE_DAYS", default_var="0") or "0")  # 0 = sem expiração

# =============================================================================
# Query (LIMIT usa MAX_ROWS)
# =============================================================================

BASE_QUERY = f"""
SELECT 
    t1.imv_nu_rip as rip_imovel,
    t1.imv_da_cadastro as data_cadastro,
    t1.imv_id_imovelspunet as id_imovel_spunet,
    t1.imv_in_migracaospunet as migracao_spunet,
    t5.no_motivocancelamentoimovel as situacao_imovel,
    t10.no_proprietariooficial as proprietario_oficial,
    t9.no_orgaoextinto as orgao_extinto,
    t12.no_tipoimovel as tipo_imovel,
    t13.no_tipovocacao as tipo_vocacao,
    t11.no_tipodominio as tipo_dominio,
    t3.no_direitoadquirido as tipo_direito_adquirido,
    t2.no_conceituacaoterreno as tipo_conceituacao_terreno,
    t7.no_naturezaterreno as tipo_natureza_terreno,
    t1.imv_ed_cep as cep,
    t1.imv_ed_tipologradouro as tipo_logradouro,
    t1.imv_ed_logradouro as logradouro,
    t1.imv_ed_numero as numero_endereco,
    t1.imv_ed_complemento as complemento_endereco,
    t1.imv_ed_bairro as bairro,
    t1.imv_in_enderecovalidado as endereco_validado,
    t1.imv_no_latitudelongitude,
    t6.no_municipio as municipio,
    t1.imv_sg_uf as uf,
    t4.no_formaaquisicao as tipo_forma_aquisicao,
    t1.imv_da_incorporacao as data_incorporacao,
    t1.imv_da_registrocartorio as data_registro_cartorio
FROM dbp_29321_spiunet.imovel t1
LEFT JOIN dbp_29321_spiunet.conceituacaoterreno t2
       ON t1.imv_co_conceituacaoterreno = t2.co_conceituacaoterreno
LEFT JOIN dbp_29321_spiunet.direitoadquirido t3
       ON t1.imv_co_direitoadquirido = t3.co_direitoadquirido
LEFT JOIN dbp_29321_spiunet.formaaquisicao t4
       ON t1.imv_co_formaaquisicao = t4.co_formaaquisicao
LEFT JOIN dbp_29321_spiunet.motivocancelamentoimovel t5
       ON t1.imv_co_motivocancelamentoimovel = t5.co_motivocancelamentoimovel
LEFT JOIN dbp_29321_spiunet.municipio t6
       ON t1.imv_co_municipio = t6.co_municipio
LEFT JOIN dbp_29321_spiunet.naturezaterreno t7
       ON t1.imv_co_naturezaterreno = t7.co_naturezaterreno
LEFT JOIN dbp_29321_spiunet.nivelrigoravaliacao t8
       ON t1.imv_co_nivelrigoravaliacao = t8.co_nivelrigoravaliacao
LEFT JOIN dbp_29321_spiunet.orgaoextinto t9
       ON t1.imv_co_orgaoextinto = t9.co_orgaoextinto
LEFT JOIN dbp_29321_spiunet.proprietariooficial t10
       ON t1.imv_co_proprietariooficial = t10.co_proprietariooficial
LEFT JOIN dbp_29321_spiunet.tipodominio t11
       ON t1.imv_co_tipodominio = t11.co_tipodominio
LEFT JOIN dbp_29321_spiunet.tipoimovel t12
       ON t1.imv_co_tipoimovel = t12.co_tipoimovel
LEFT JOIN dbp_29321_spiunet.tipovocacao t13
       ON t1.imv_co_tipovocacao = t13.co_tipovocacao
LIMIT {MAX_ROWS}
"""

# =============================================================================
# Helpers
# =============================================================================

_illegal_re = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

def _sanitize_value(v):
    if v is None:
        return v
    if isinstance(v, str):
        return _illegal_re.sub("", v)
    return v

def _get_impala_params_from_conn(conn_id: str):
    """Converte a Connection 'generic' do Airflow em kwargs p/ impyla.connect"""
    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson or {}
    params = {
        "host": conn.host,
        "port": conn.port or 21050,
        "user": conn.login,
        "password": conn.password,
        "auth_mechanism": extras.get("auth_mechanism") or extras.get("extra__generic__auth_mechanism") or "PLAIN",
        "use_ssl": extras.get("use_ssl") in (True, "true", "True", "1") or extras.get("extra__generic__use_ssl") in (True, "true", "True", "1"),
    }
    ca = extras.get("ca_cert") or extras.get("extra__generic__ca_cert")
    if params["use_ssl"] and ca:
        params["ca_cert"] = ca
    return params

# ------------------ Nextcloud (WebDAV + OCS) ------------------

def _webdav_url_for(path: str) -> str:
    # path relativo ao root do usuário (sem barra inicial)
    safe_path = "/".join(quote(p) for p in path.split("/")) if path else ""
    return f"{NC_BASE}/remote.php/dav/files/{quote(NC_USER)}/{safe_path}"

def _ocs_url(path: str) -> str:
    # API OCS (para shares)
    return f"{NC_BASE}/ocs/v2.php{path}"

def _make_session() -> requests.Session:
    s = requests.Session()
    s.auth = HTTPBasicAuth(NC_USER, NC_PASS)
    s.verify = VERIFY_SSL
    return s

def _ensure_directory(session: requests.Session, dir_path: str):
    """Cria diretórios recursivamente com MKCOL (idempotente)."""
    path = (dir_path or "").strip("/")
    if not path:
        return
    parts = path.split("/")
    current = ""
    for part in parts:
        current = f"{current}/{part}" if current else part
        url = _webdav_url_for(current)
        resp = session.request("MKCOL", url)
        if resp.status_code in (201, 405):
            continue
        # Verificação de existência (PROPFIND depth=0)
        check = session.request("PROPFIND", url, headers={"Depth": "0"})
        if check.status_code not in (200, 207):
            raise RuntimeError(f"Falha ao criar diretório {current}: {resp.status_code} {resp.text}")

def _upload_file(session: requests.Session, local_path: str, remote_rel_path: str):
    """PUT sobrescreve o arquivo remoto sem alterar o share (link permanece)."""
    url = _webdav_url_for(remote_rel_path)
    with open(local_path, "rb") as f:
        resp = session.put(url, data=f)
    if resp.status_code not in (200, 201, 204):
        raise RuntimeError(f"Falha no upload {remote_rel_path}: {resp.status_code} {resp.text}")

def _get_existing_public_share_for_path(session: requests.Session, rel_path: str):
    """
    Retorna o primeiro share público (share_type=3) do ARQUIVO exato ou None.
    """
    headers = {"OCS-APIRequest": "true", "Accept": "application/json"}
    params = {"path": f"/{rel_path.strip('/')}", "format": "json"}
    url = _ocs_url("/apps/files_sharing/api/v1/shares")
    resp = session.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        raise RuntimeError(f"OCS shares GET falhou: {resp.status_code} {resp.text}")
    data = resp.json()
    entries = data.get("ocs", {}).get("data", []) if data else []
    for e in entries:
        if str(e.get("share_type")) == "3" and e.get("item_type") == "file":
            # Confere caminho do item
            if e.get("path") == f"/{rel_path.strip('/')}":
                return e
    return None

def _create_public_share(session: requests.Session, rel_path: str, password: str | None, expire_days: int = 0):
    """Cria link público (shareType=3) para arquivo. Retorna dict com info do share."""
    headers = {"OCS-APIRequest": "true", "Accept": "application/json"}
    url = _ocs_url("/apps/files_sharing/api/v1/shares")
    payload = {
        "path": f"/{rel_path.strip('/')}",
        "shareType": 3,      # link público
        "permissions": 1,    # leitura
    }
    if password:
        payload["password"] = password
    if expire_days and expire_days > 0:
        exp = date.today() + timedelta(days=expire_days)
        payload["expireDate"] = exp.isoformat()

    resp = session.post(url, headers=headers, data=payload)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"OCS share POST falhou: {resp.status_code} {resp.text}")
    data = resp.json()
    meta = data.get("ocs", {}).get("meta", {})
    if meta.get("status") != "ok":
        raise RuntimeError(f"OCS share POST erro: {json.dumps(data)}")
    return data["ocs"]["data"]

def _update_public_share(session: requests.Session, share_id: str, password: str | None, expire_days: int = 0):
    """Atualiza senha e/ou expiração de share existente."""
    headers = {"OCS-APIRequest": "true", "Accept": "application/json"}
    url = _ocs_url(f"/apps/files_sharing/api/v1/shares/{share_id}")
    if password is not None:
        resp = session.put(url, headers=headers, data={"password": password})
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"OCS share PUT (password) falhou: {resp.status_code} {resp.text}")
    if expire_days and expire_days > 0:
        exp = date.today() + timedelta(days=expire_days)
        resp = session.put(url, headers=headers, data={"expireDate": exp.isoformat()})
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"OCS share PUT (expireDate) falhou: {resp.status_code} {resp.text}")

# =============================================================================
# Tasks
# =============================================================================

def _run_impala_query_to_excel(**_):
    from impala.dbapi import connect

    kwargs = _get_impala_params_from_conn(CONN_ID)
    with connect(**kwargs) as impala_conn:
        df = pd.read_sql(BASE_QUERY, impala_conn)

    # Sanitize para Excel
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(_sanitize_value)

    df.to_excel(OUTPUT_FILE, index=False)

def _publish_to_nextcloud(**context):
    """
    Garante pasta no Nextcloud, realiza PUT (sobrescreve o mesmo arquivo) e
    cria/atualiza link público de ARQUIVO. Empurra URLs via XCom.
    """
    ti = context["ti"]

    remote_dir = (NC_DIR or "").strip("/")
    remote_rel_path = f"{remote_dir}/{OUTPUT_FILENAME}" if remote_dir else OUTPUT_FILENAME

    session = _make_session()

    # 1) Garante diretório (recursivo)
    _ensure_directory(session, remote_dir)

    # 2) Upload (PUT) sobrescrevendo para manter o mesmo share ID
    _upload_file(session, OUTPUT_FILE, remote_rel_path)

    # 3) Share público do ARQUIVO (criar uma vez; depois reaproveitar)
    existing = _get_existing_public_share_for_path(session, remote_rel_path)
    if existing:
        share_id = str(existing.get("id"))
        _update_public_share(session, share_id, NC_SHARE_PASSWORD, NC_SHARE_EXPIRE_DAYS)
        share_url = existing.get("url")
    else:
        created = _create_public_share(session, remote_rel_path, NC_SHARE_PASSWORD, NC_SHARE_EXPIRE_DAYS)
        share_id = str(created.get("id"))
        share_url = created.get("url")

    # 4) Link direto para download
    direct_download_url = f"{share_url}/download" if share_url else None

    # XComs para o e-mail
    ti.xcom_push(key="nc_share_url", value=share_url)
    ti.xcom_push(key="nc_direct_download_url", value=direct_download_url)
    ti.xcom_push(key="nc_remote_path", value=remote_rel_path)

def _send_mail(**context):
    ti = context["ti"]
    share_url = ti.xcom_pull(key="nc_share_url", task_ids="publish_to_nextcloud")
    direct_download_url = ti.xcom_pull(key="nc_direct_download_url", task_ids="publish_to_nextcloud")
    remote_path = ti.xcom_pull(key="nc_remote_path", task_ids="publish_to_nextcloud")

    msg = EmailMessage()
    msg["Subject"] = MAIL_SUBJECT
    msg["From"] = MAIL_FROM_HEADER
    msg["To"] = ", ".join(MAIL_TO)
    msg["Reply-To"] = MAIL_REPLY_TO
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain=MAIL_EHLO_HOST)

    linhas = [
        "Olá,",
        "",
        "Segue o relatório gerado do Impala.",
        f"- Linhas (máx): {MAX_ROWS}",
        f"- Publicado no Nextcloud em: /{remote_path}" if remote_path else "",
        f"- Link público: {share_url}" if share_url else "",
        f"- Download direto: {direct_download_url}" if direct_download_url else "",
        f"- Senha do link: {NC_SHARE_PASSWORD}" if NC_SHARE_PASSWORD else "",
        f"- Expira em ~{NC_SHARE_EXPIRE_DAYS} dia(s)." if NC_SHARE_EXPIRE_DAYS > 0 else "",
        "",
        "[]'s",
        "Airflow",
    ]
    msg.set_content("\n".join([l for l in linhas if l != ""]))

    # Mantém o anexo (opcional)
    with open(OUTPUT_FILE, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=OUTPUT_FILENAME,
        )

    with smtplib.SMTP(MAIL_SERVER, MAIL_PORT, timeout=60) as smtp:
        smtp.set_debuglevel(1)
        smtp.ehlo(MAIL_EHLO_HOST)
        smtp.send_message(msg, from_addr=MAIL_FROM_ENVELOPE, to_addrs=MAIL_TO)

# =============================================================================
# DAG / Operators
# =============================================================================

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id=DAG_ID,
    description=DESCRIPTION,
    start_date=START_DATE,
    schedule_interval=SCHEDULE,
    catchup=False,
    default_args=default_args,
    tags=["email", "excel", "impala", "nextcloud", "webdav"],
) as dag:

    run_impala_query_to_excel = PythonOperator(
        task_id="run_impala_query_to_excel",
        python_callable=_run_impala_query_to_excel,
    )

    publish_to_nextcloud = PythonOperator(
        task_id="publish_to_nextcloud",
        python_callable=_publish_to_nextcloud,
        provide_context=True,
    )

    send_email_with_attachment = PythonOperator(
        task_id="send_email_with_attachment",
        python_callable=_send_mail,
        provide_context=True,
    )

    run_impala_query_to_excel >> publish_to_nextcloud >> send_email_with_attachment
