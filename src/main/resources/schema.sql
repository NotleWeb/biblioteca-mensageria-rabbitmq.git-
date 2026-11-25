CREATE TABLE IF NOT EXISTS eventos_processados(
  consumidor VARCHAR(40), event_id VARCHAR(80), instancia VARCHAR(40),
  processado_em TIMESTAMP DEFAULT now(), PRIMARY KEY (consumidor, event_id));
CREATE TABLE IF NOT EXISTS circulacao_saidas(
  id SERIAL PRIMARY KEY, event_id VARCHAR(80), exemplar_id VARCHAR(40),
  leitor_id VARCHAR(40), data_prevista_devolucao DATE);
CREATE TABLE IF NOT EXISTS avisos(
  id SERIAL PRIMARY KEY, event_id VARCHAR(80), leitor_id VARCHAR(40),
  tipo_aviso VARCHAR(30), mensagem VARCHAR(300), criado_em TIMESTAMP DEFAULT now());
CREATE TABLE IF NOT EXISTS multas(
  id SERIAL PRIMARY KEY, event_id VARCHAR(80), multa_id VARCHAR(40), leitor_id VARCHAR(40),
  valor NUMERIC(10,2), status VARCHAR(20), criado_em TIMESTAMP DEFAULT now());
CREATE TABLE IF NOT EXISTS auditoria(
  event_id VARCHAR(80) PRIMARY KEY, tipo VARCHAR(40), ocorrido_em TIMESTAMP,
  payload TEXT, registrado_em TIMESTAMP DEFAULT now());
CREATE TABLE IF NOT EXISTS estatisticas(
  categoria VARCHAR(60) PRIMARY KEY, total BIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS estatisticas_log(
  id SERIAL PRIMARY KEY, event_id VARCHAR(80), instancia VARCHAR(40),
  categoria VARCHAR(60), processado_em TIMESTAMP DEFAULT now());
