sequenceDiagram
    participant Scheduler
    participant Fila as Fila de Processamento
    participant Worker
    participant API as APIs Open Finance
    participant BD as Banco de Dados
    participant Notificacao as Serviço de Notificação

    Scheduler->>Fila: 1. Adiciona usuários ativos na fila
    Note over Scheduler,Fila: Toda quinta-feira
    Fila->>Worker: 2. Consome usuários da fila
    Worker->>API: 3. Faz chamada às APIs de Open Finance
    API-->>Worker: 4. Retorna dados atualizados
    Worker->>BD: 5. Grava dados no banco de dados
    alt Dados gravados com sucesso
        Worker->>Notificacao: 6. Envia notificação (opcional)
        Notificacao-->>Worker: Confirmação
    else Falha na gravação
        Worker->>Fila: 6. Reinsere usuário na fila
    end


sequenceDiagram
    participant Usuario as Usuário (App)
    participant Backend as Backend (Serviços)
    participant Autenticacao as Serviço de Autenticação
    participant API as APIs Open Finance
    participant BD as Banco de Dados

    Usuario->>Backend: 1. Faz login no app
    Backend->>Autenticacao: 2. Valida credenciais
    Autenticacao-->>Backend: 3. Retorna token de acesso
    Backend->>API: 4. Faz chamada às APIs de Open Finance
    API-->>Backend: 5. Retorna dados atualizados (contas, cartões, transações)
    Backend->>BD: 6. Grava dados no banco de dados
    Backend-->>Usuario: 7. Exibe dados atualizados no app
