# Case PAN

## 1. Introdução

Nesse case busquei solucionar o problema de negócio apresentado usando dentro das restrições estabelicidades e impostas
gerar um book de variáveis contendo:
    - Manipulação mínima de valores inconsistentes de entrada
    - Criação de tabelas intermediárias para proporcionar melhores testes unitários
    - Criação de uma tabela agregada
    - Criação de uma tabela com variáveis extras

## 2. Sobre o material

Nessa pasta se encontram alguns arquivos necessários para entendimento do processo.
Procurei implementar apenas 2 classes. Como passo posterior penso que seria interessante implementar no mínimo
mais 1.

Folder:
    - db : contém a classe implementada para instanciar o banco de dados com pattern singleton
    - data_handler : contém a classe que lida com os dados em sua totalidade de forma programática
    - raw_files : arquivos originais enviados para realização do case

Arquivos:
    - global_params.yaml : arquivo YAML para diminuir manipulação extensa do código bem como facilitar escalabilidade
    - db_exploration.ipynb : notebook para entendimento inicial da tabela
    - requirements.txt : arquivo de requisitos. Note que precisei inserir os requisitos do JUPYTER SERVER para ler no VS Code.
    - env : Virtual Env usado no projeto.

## 3. Observações

O case foi consideravelmente extenso, na parte de escrever e popular a 'feature store' acabei fazendo em duas etapas

1- Escreve uma tabela gerada de modo procedural
2- Popula com variáveis extras.

Apesar de não gostar dessa forma, resolve o problema. Sugestão de implementação: Refatorar todo esse processo para que 
uma query mais elaborada possa popular esses dados de forma procedural. Tentei fazer por esse caminho mas não conclui.
O meu racional seria: construir uma tabela com chaves unicas de cpf e data > alterar a tabela de modo procedural >
atualizar os valores via condição.
Na minha cabeça funcionou bem, mas a implementação não foi simples como pensei e adotei o modo "força bruta", não entendi exatamente
o porque não deu certo, mas não sou muito familizariado com essa manipulação com os vínculos do SQL em si somadas à biblioteca do SQLite,
que também tenho menos familiaridade.

## 4. Novos passos

- 1. Implementar uma nova classe para lidar com a tratativa de dados
- 2. Refatorar a classe de DataHandler para ser mais flexível e escalável
- 3. Implementar novas variáveis:
    - SHARE OF WALLET baseado no valor tomado interno em comparação com o de mercado.
    - Flags de inadimplencia de mercado olhando janelas
- 4. Implementar testes unitários e integrados
- 5. Implementar tratamento de exceções
- 6. Refatorar o código para ele ser capaz de dar carga inicial e cargas adicionais baseada numa nova partição (data consulta)

## 5. Novas modalidades

Para inserir novas modalidades basta alterar o arquivo global_params.yaml com os dados de novas modalidade
no campo 'cod_modalidade' e executar o código do main.py