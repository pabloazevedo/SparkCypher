# Spark + Morpheus + Scala + MongoDB
# MorpheusScala

Este projeto é um exemplo simples de processamento com grafos usando Spark e Cypher (versão opensoruce até então era chamda Morpheus).
Abaixo um exemplo já processado:

1. persons.csv
2. movies.csv
3. acted_in.csv

Basicamente, este exemplo processa os dados com Spark e salva o resultado em um container com MongoDB para vizualização (não implementei).

```bash
docker pull tutum/mongodb
docker run -d -p 27017:27017 -p 28017:28017 -e AUTH=no tutum/mongodb
docker start docker run -d -p 27017:27017 -p 28017:28017 -e AUTH=no tutum/mongodb
docker start <id_do_seu_container>
```

Para poder conectar também foi preciso instalar o cliente.
```bash
sudo apt install mongodb-clients
```