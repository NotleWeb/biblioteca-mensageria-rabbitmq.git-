#!/usr/bin/env bash
# Procedimento de teste (produtor em :8080, estatisticas em :8085/:8086)
P=http://localhost:8080/eventos
echo "== 1. Os 4 eventos"
curl -s -X POST $P/emprestimo-registrado; echo
curl -s -X POST $P/exemplar-devolvido; echo
curl -s -X POST $P/multa-aplicada; echo
curl -s -X POST $P/multa-cancelada; echo
echo "== 3. Concorrencia: 10 emprestimos distintos"
for i in $(seq 1 10); do curl -s -X POST $P/emprestimo-registrado > /dev/null; done
echo "== 5. Duplicidade: mesmo eventId 2x (deve contar 1 vez)"
BODY='{"emprestimoId":"E9999","exemplarId":"X999","titulo":"Teste","categoria":"Teste-Duplicidade","leitorId":"L999","dataPrevistaDevolucao":"2026-12-31"}'
for i in 1 2; do curl -s -X POST "$P/emprestimo-registrado?eventId=evento-dup-001" -H 'Content-Type: application/json' -d "$BODY" > /dev/null; done
sleep 2
curl -s http://localhost:8085/consulta/estatisticas; echo
curl -s http://localhost:8085/consulta/estatisticas_log | head -c 1500; echo
