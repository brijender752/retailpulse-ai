# RetailPulse AI — GenAI data flow

This diagram describes the current source implementation. Runtime validation of the analytics endpoint and its agent integration remains pending; the latest reported issue was reporting-view setup or permissions.

```mermaid
flowchart TB
    U[User / Swagger / API client]

    subgraph APIs[FastAPI entry points]
        C[POST /ai/customer]
        A[POST /agent/chat]
        S[POST /analytics-ai/query]
        I[POST /ai/knowledge/ingest]
    end
    U -->|Customer ID + question| C
    U -->|Question + optional session ID| A
    U -->|Aggregate business question| S
    U -->|Trigger knowledge indexing| I

    subgraph Agent[Phase 4.4 agent]
        O[Agent orchestrator]
        M[(PostgreSQL: ai.conversations and ai.messages)]
        L[Ollama / Qwen3:4b: tool selection and answer generation]
        T[Tool dispatcher: validate arguments]
        CT[get_customer_intelligence]
        XT[get_churn_explanation]
        KT[search_retailpulse_knowledge]
        ST[query_analytics]
        F[Answer + session_id + tools_used]
        A --> O
        M -->|Last 12 messages| O
        O -->|System rules + history + question + tool schemas| L
        L -->|Tool calls| T
        T --> CT
        T --> XT
        T --> KT
        T --> ST
        CT -->|Customer facts| O
        XT -->|Stored SHAP contributions or unavailable| O
        KT -->|Relevant document excerpts| O
        ST -->|Analytics result or unsupported reason| O
        O -->|Tool results for next round| L
        L -->|Final text| O
        O -->|Save user question and final answer| M
        O --> F
        F --> U
    end

    subgraph Customer[Customer assistant]
        CO[Customer orchestrator]
        CP[Structured facts + retrieved evidence + question]
        CL[Ollama / Qwen3:4b]
        CR[Answer + context_used + sources]
        D[Direct recommendation response from stored rankings]
        C --> CO
        CO --> CT
        CO -->|General customer question| R
        CT -->|Customer data| CP
        R -->|Relevant excerpts| CP
        CO -->|Build prompt for general question| CP
        CP --> CL --> CR --> U
        CO -->|Recognized direct recommendation question| D --> U
        CO -->|Customer absent| NF[Customer not found response]
        NF --> U
    end

    subgraph Knowledge[Knowledge ingestion and retrieval]
        DOC[Mounted /knowledge/*.md files]
        CH[Markdown chunks + source and heading]
        EM[Ollama / nomic-embed-text]
        Q[(Qdrant: retailpulse_knowledge)]
        R[Embed question and retrieve similar chunks]
        I -->|Read documents| DOC --> CH
        CH -->|Chunk text| EM
        EM -->|Chunk vectors + metadata| Q
        KT -->|Question| R
        R -->|Question text| EM
        EM -->|Question vector| R
        R -->|Cosine search| Q
        Q -->|Chunks passing relevance threshold| R
    end

    subgraph Analytics[Read-only text-to-SQL]
        AS[answer_analytics_question]
        G[Ollama / Qwen3:4b: question + approved schema to JSON]
        UNS[Unsupported response with reason]
        V[SQLGlot validation: one SELECT, approved view and columns, row cap]
        EX[Read-only executor: retailpulse_analyst, 5-second statement timeout]
        VW[(ai_reporting.customer_metrics)]
        RES[Serialized SQL + columns + rows + row_count]
        S --> AS
        ST --> AS
        AS --> G
        G -->|Unsupported| UNS --> AS
        G -->|Supported SQL| V --> EX
        EX -->|Validated SQL| VW
        VW -->|Result rows| EX --> RES --> AS
        AS -->|Standalone JSON response| S
        S -->|Response or error| U
    end

    DB[(PostgreSQL serving tables: customer_360, churn_predictions, product_recommendations)]
    SH[(PostgreSQL: serving.churn_explanations)]
    DB -->|Parameterized customer queries| CT
    SH -->|Latest stored explanation| XT
    DB -->|customer_360 joined to churn_predictions| VW
```

## Implementation details

- The agent allows four model rounds and up to three tool calls per round. Conversation memory stores user and assistant messages; the returned tool trace records tool names and arguments.
- Knowledge ingestion rebuilds the configured Qdrant collection. Retrieval filters cosine similarity scores below 0.3; the customer assistant requests four chunks, and the agent knowledge tool requests two.
- Customer lookup and SHAP tools use fixed parameterized SQL. Only the analytics tool generates SQL, which passes through SQLGlot validation before execution with the analyst database account.
- Analytics is limited to the approved customer reporting view and 100 rows. Product revenue by month is unsupported because the view lacks products and transaction dates.
- SHAP contributions, churn predictions, and product rankings are precomputed data read by these flows; the GenAI request does not train or run those underlying models.
- Ollama supplies both chat generation and embeddings using separate models. Defaults are `qwen3:4b` and `nomic-embed-text`.

## Source map

| Component | Source |
| --- | --- |
| API router registration | `api/app/main.py` |
| Customer assistant | `api/app/ai/orchestrator.py` |
| Customer database queries | `api/app/ai/tools.py` |
| Agent orchestration | `api/app/agent/orchestrator.py` |
| Agent tools | `api/app/agent/tools.py` |
| Conversation persistence | `api/app/agent/memory.py` |
| Knowledge ingestion and retrieval | `api/app/rag/` |
| Analytics generation, validation and execution | `api/app/text2sql/` |
| Approved reporting view | `sql/ai_reporting.sql` |
| Analyst role setup | `sql/create_analyst_role.sql` |
