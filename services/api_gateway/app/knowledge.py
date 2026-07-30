"""
Knowledge ingestion logic for the RAG pipeline.

Responsibilities:
1. Prepare incident text for chunking (combine relevant fields)
2. Chunk text using RecursiveCharacterTextSplitter
3. Generate embeddings via AWS Bedrock (Cohere Embed v4)
4. Store chunks + embeddings in ChromaDB
5. Store raw incident record in PostgreSQL
"""

import asyncio
import json
import logging
from uuid import UUID, uuid4

import asyncpg
import boto3
import chromadb
from langchain_text_splitters import RecursiveCharacterTextSplitter

from services.api_gateway.app.config import settings
from shared.models.models import IncidentCreate

logger = logging.getLogger("api_gateway")

# --- Text Splitter (configured once, reused) ---
# 512 tokens ≈ ~2048 characters for English text (rough 1:4 ratio)
# 50 token overlap ≈ 200 characters ensures context continuity between chunks
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=2048,
    chunk_overlap=200,
    length_function=len,
    separators=["\n\n", "\n", ". ", ", ", " ", ""],
)


def _prepare_incident_text(incident: IncidentCreate) -> str:
    """
    Combine incident fields into a single text document for chunking.

    We concatenate the most semantically rich fields — this becomes
    the text that gets embedded and retrieved during diagnosis.
    """
    resolution_text = "\n".join(f"- {step}" for step in incident.resolution_steps)

    return (
        f"Incident: {incident.title}\n\n"
        f"Description: {incident.description}\n\n"
        f"Root Cause: {incident.root_cause}\n\n"
        f"Resolution Steps:\n{resolution_text}\n\n"
        f"Affected Device Types: {', '.join(incident.affected_device_types)}\n"
        f"Severity: {incident.severity}\n"
        f"Category: {incident.failure_category}"
    )


def _generate_embeddings(texts: list[str]) -> list[list[float]]:
    """
    Generate embeddings for a list of texts.

    Uses AWS Bedrock (Cohere Embed v4) by default, or ChromaDB's built-in
    local model (all-MiniLM-L6-v2) when EMBEDDING_PROVIDER=local.

    Uses boto3 directly rather than LangChain's BedrockEmbeddings because:
    - Inference profile ARNs require explicit request body formatting
    - Cohere Embed v4 needs input_type parameter for optimal results
    - Direct boto3 gives us full control and better error messages
    """
    if settings.embedding_provider == "local":
        from chromadb.utils.embedding_functions import DefaultEmbeddingFunction

        ef = DefaultEmbeddingFunction()
        return ef(texts)

    # Bedrock Cohere Embed v4
    client = boto3.client("bedrock-runtime", region_name=settings.aws_region)
    response = client.invoke_model(
        modelId=settings.claude_embedding_model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(
            {
                "texts": texts,
                "input_type": "search_document",
            }
        ),
    )
    response_body = json.loads(response["body"].read())
    return response_body["embeddings"]["float"]


def _generate_query_embedding(text: str) -> list[float]:
    """
    Generate a single embedding for a query string.

    Uses AWS Bedrock (Cohere Embed v4) by default, or ChromaDB's built-in
    local model (all-MiniLM-L6-v2) when EMBEDDING_PROVIDER=local.

    Uses input_type="search_query" for optimal retrieval performance.

    Cohere Embed v4 distinguishes between document and query embeddings:
    - search_document: used when indexing/storing content
    - search_query: used when searching for relevant content
    This asymmetry improves retrieval accuracy.
    """
    if settings.embedding_provider == "local":
        from chromadb.utils.embedding_functions import DefaultEmbeddingFunction

        ef = DefaultEmbeddingFunction()
        return ef([text])[0]

    client = boto3.client("bedrock-runtime", region_name=settings.aws_region)
    response = client.invoke_model(
        modelId=settings.claude_embedding_model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(
            {
                "texts": [text],
                "input_type": "search_query",
            }
        ),
    )
    response_body = json.loads(response["body"].read())
    return response_body["embeddings"]["float"][0]


def _sync_chunk_embed_upsert(
    incident: IncidentCreate,
    incident_id: UUID,
    chroma_client: chromadb.ClientAPI,
) -> tuple[int, int]:
    """
    Synchronous pipeline: chunk → embed → upsert. Runs in a thread pool
    via asyncio.to_thread() to avoid blocking the async event loop.

    Steps:
        1. Prepare combined text from incident fields
        2. Split into chunks using RecursiveCharacterTextSplitter
        3. Generate embeddings via AWS Bedrock (Cohere Embed v4)
        4. Upsert chunks + embeddings into ChromaDB with incident metadata

    Args:
        incident: The incident document to ingest.
        incident_id: Pre-generated UUID for this incident (used as chunk ID prefix
            and metadata link back to the PostgreSQL record).
        chroma_client: ChromaDB HTTP client for vector store operations.

    Returns:
        Tuple of (chunk_count, collection_total) where chunk_count is the number
        of chunks created for this incident and collection_total is the total
        number of documents in the ChromaDB collection after upsert.
    """
    full_text = _prepare_incident_text(incident)
    logger.info(f"Ingesting incident '{incident.title}' ({len(full_text)} chars)")

    chunks = text_splitter.split_text(full_text)
    chunk_count = len(chunks)
    logger.info(f"Split into {chunk_count} chunks")

    embeddings = _generate_embeddings(chunks)

    collection = chroma_client.get_or_create_collection(
        name="incident_embeddings",
        metadata={"hnsw:space": "cosine"},
    )

    chunk_ids = [f"{incident_id}_chunk_{i}" for i in range(chunk_count)]
    metadatas = [
        {
            "incident_id": str(incident_id),
            "chunk_index": i,
            "title": incident.title,
            "severity": incident.severity,
            "failure_category": incident.failure_category,
            "affected_device_types": json.dumps(incident.affected_device_types),
        }
        for i in range(chunk_count)
    ]

    collection.upsert(
        ids=chunk_ids,
        embeddings=embeddings,
        documents=chunks,
        metadatas=metadatas,
    )

    return chunk_count, collection.count()


async def ingest_incident(
    incident: IncidentCreate,
    conn: asyncpg.Connection,
    chroma_client: chromadb.ClientAPI,
) -> tuple[UUID, int]:
    """
    Full ingestion pipeline for a single incident document.

    Returns:
        Tuple of (incident_id, chunk_count)

    Steps:
        1. Prepare text from incident fields
        2. Chunk the text
        3. Generate embeddings for all chunks (single Bedrock API call)
        4. Upsert chunks + embeddings into ChromaDB
        5. Store raw incident in PostgreSQL
    """
    incident_id = uuid4()

    chunk_count, collection_size = await asyncio.to_thread(
        _sync_chunk_embed_upsert, incident, incident_id, chroma_client
    )

    # --- Persist to PostgreSQL ---
    await conn.execute(
        """
        INSERT INTO incidents_knowledge
            (incident_id, title, description, affected_device_types,
             root_cause, resolution_steps, severity, failure_category,
             tags, chunk_count)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """,
        incident_id,
        incident.title,
        incident.description,
        json.dumps(incident.affected_device_types),
        incident.root_cause,
        json.dumps(incident.resolution_steps),
        incident.severity,
        incident.failure_category,
        json.dumps(incident.tags),
        chunk_count,
    )

    logger.info(
        f"Incident ingested: id={incident_id}, chunks={chunk_count}, "
        f"ChromaDB collection size={collection_size}"
    )

    return incident_id, chunk_count
