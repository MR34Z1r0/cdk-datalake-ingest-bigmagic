from typing import Optional, Dict, List, Any, Union
import json
import boto3
from pinecone import Pinecone
from botocore.exceptions import ClientError

from ...common.logger import custom_logger

logger = custom_logger(__name__)


class PineconeHelper:
    """Custom helper for Pinecone Serverless to simplify vector search operations."""

    def __init__(
        self,
        index_name: str,
        api_key: str,
        embeddings_model_id: str,
        embeddings_region: str,
        max_retrieve_documents: int = 5,
        min_threshold: float = 0.3,
    ) -> None:
        """
        Initialize the Pinecone helper for serverless operation.

        :param index_name: Name of the Pinecone index.
        :param api_key: Pinecone API key (required).
        :param embeddings_model_id: Bedrock embeddings model ID (required).
        :param embeddings_region: AWS region for Bedrock (required).
        :param max_retrieve_documents: Maximum number of documents to retrieve (default: 5).
        :param min_threshold: Minimum similarity threshold for results (default: 0.3).
        """
        self.index_name = index_name
        self.api_key = api_key
        self.embeddings_model_id = embeddings_model_id
        self.embeddings_region = embeddings_region
        self.max_retrieve_documents = max_retrieve_documents
        self.min_threshold = min_threshold
        
        # Initialize AWS client for embeddings
        self.bedrock_client = boto3.client("bedrock-runtime", region_name=self.embeddings_region)
        
        # Initialize Pinecone client (Serverless-compatible)
        self.pinecone_client = Pinecone(api_key=self.api_key)
        self.index = self.pinecone_client.Index(self.index_name)
        self._validate_index()
        logger.info(f"Configured helper for Pinecone serverless index: {index_name}")

    def _validate_index(self) -> None:
        """Validate that the Pinecone index exists and is accessible."""
        try:
            self.index.describe_index_stats()
            logger.info(f"Successfully connected to Pinecone index: {self.index_name}")
        except Exception as error:
            logger.error(f"Index {self.index_name} does not exist or is inaccessible")
            raise error

    def get_embeddings(self, text: str) -> List[float]:
        """
        Get embeddings for the given text using AWS Bedrock.
        
        :param text: Text to get embeddings for.
        :return: List of embedding values.
        """
        logger.info(f"Getting embeddings for text: {text[:50]}...")
        try:
            response = self.bedrock_client.invoke_model(
                body=json.dumps({"inputText": text}),
                modelId=self.embeddings_model_id
            )
            response_body = json.loads(response["body"].read())
            embeddings = response_body.get("embedding", [])
            logger.info("Embeddings obtained successfully")
            return embeddings
        except ClientError as error:
            logger.error(
                f"Failed to obtain embeddings | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error
        except Exception as error:
            logger.error(f"Error getting embeddings: {str(error)}")
            raise error

    def query(
        self, 
        embeddings: List[float],
        filter_conditions: Optional[Dict[str, Any]] = None,
        top_k: Optional[int] = None,
        include_metadata: bool = True,
        namespace: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Query Pinecone index with embeddings and optional filters.
        
        :param embeddings: Vector embeddings to query with.
        :param filter_conditions: Optional dictionary of filter conditions.
        :param top_k: Maximum number of results to return (default: class max_retrieve_documents).
        :param include_metadata: Whether to include metadata in results (default: True).
        :param namespace: Optional namespace to query in.
        :return: List of matching documents.
        """
        logger.info(f"Querying Pinecone index: {self.index_name}")
        top_k = top_k or self.max_retrieve_documents
        
        try:
            kwargs = {
                "vector": embeddings,
                "top_k": top_k,
                "include_metadata": include_metadata
            }
            
            if filter_conditions:
                kwargs["filter"] = filter_conditions
                
            if namespace:
                kwargs["namespace"] = namespace
            
            logger.info(f"kwargs: {kwargs}")    
            response = self.index.query(**kwargs)
            results = response.get("matches", [])
            logger.info(f"Found {len(results)}")    
            logger.info(f"Details: {results}")
            # Filter by threshold
            filtered_results = [match for match in results if match["score"] >= self.min_threshold]
            logger.info(f"Found {len(filtered_results)} matches above threshold in Pinecone")
            
            # Log detailed results
            for idx, match in enumerate(filtered_results, start=1):
                score = match["score"]
                vector_id = match["id"]
                logger.debug(f"[{idx}] ID: {vector_id} | Score: {score:.4f}")
            
            return filtered_results
        except Exception as error:
            logger.error(f"Query failed - Index: {self.index_name} | Error: {str(error)}")
            raise error

    def search_by_text(
        self, 
        query_text: str,
        filter_conditions: Optional[Dict[str, Any]] = None,
        top_k: Optional[int] = None,
        include_metadata: bool = True,
        namespace: Optional[str] = None,
        return_format: str = "raw",
        text_field: str = "text"
    ) -> Union[str, List[Dict[str, Any]]]:
        """
        Search index using text query - converts to embeddings and searches.
        
        :param query_text: Text to search for.
        :param filter_conditions: Optional filters to apply to the search.
        :param top_k: Maximum number of results to return.
        :param include_metadata: Whether to include metadata in results.
        :param namespace: Optional namespace to query in.
        :param return_format: Format to return results ('text', 'raw').
        :param text_field: Field name in metadata containing text (for 'text' return format).
        :return: Either raw results or concatenated text from results.
        """
        logger.info(f"Searching by text: {query_text[:50]}...")
        
        # Get embeddings for the query
        embeddings = self.get_embeddings(query_text)
        
        # Query Pinecone
        results = self.query(
            embeddings=embeddings, 
            filter_conditions=filter_conditions,
            top_k=top_k,
            include_metadata=include_metadata,
            namespace=namespace
        )
        
        # Return results based on format
        if return_format == "raw":
            return results
        elif return_format == "text":
            return '\n'.join(
                match.get("metadata", {}).get(text_field, "").replace("\n", " ").replace("  ", " ")
                for match in results
                if text_field in match.get("metadata", {})
            )
        else:
            logger.warning(f"Unknown return format: {return_format}, returning raw results")
            return results

    def upsert_vectors(
        self, 
        vectors: List[Dict[str, Any]], 
        namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Upsert vectors into the Pinecone index.
        
        :param vectors: List of vectors with id, values, and metadata.
        :param namespace: Optional namespace for the vectors.
        :return: Response from Pinecone.
        """
        logger.info(f"Upserting {len(vectors)} vectors to index {self.index_name}")
        try:
            kwargs = {"vectors": vectors}
            if namespace:
                kwargs["namespace"] = namespace
                
            response = self.index.upsert(**kwargs)
            
            # Para debugging, imprimamos qué tipo de respuesta estamos recibiendo
            logger.debug(f"Response type: {type(response)}")
            logger.debug(f"Response attributes: {dir(response)}")
            
            # Handle Pinecone serverless response format
            upserted_count = 0
            
            # Intento 1: Si es un diccionario
            if isinstance(response, dict):
                upserted_count = response.get("upserted_count", 0)
                logger.debug(f"Response is dict, upserted_count: {upserted_count}")
            # Intento 2: Si tiene el atributo upserted_count directamente
            elif hasattr(response, "upserted_count"):
                upserted_count = getattr(response, "upserted_count")
                logger.debug(f"Response has upserted_count attribute: {upserted_count}")
            # Intento 3: Si es una respuesta de tipo UpsertResponse
            elif hasattr(response, "UpsertResponse"):
                if hasattr(response.UpsertResponse, "upserted_count"):
                    upserted_count = response.UpsertResponse.upserted_count
                else:
                    # Buscar en los atributos anidados
                    upserted_count = getattr(response.UpsertResponse, "upserted_count", 0)
            # Intento 4: Si no podemos obtener el conteo, usar la longitud de vectors
            else:
                logger.warning(f"Could not determine upserted count from response type: {type(response)}")
                upserted_count = len(vectors)
            
            logger.info(f"Successfully upserted {upserted_count} vectors")
            
            # Construir respuesta consistente
            return {
                "upserted_count": upserted_count,
                "status": "success"
            }
            
        except Exception as error:
            logger.error(f"Failed to upsert vectors - Index: {self.index_name} | Error: {str(error)}")
            raise error

    def delete_vectors(
        self, 
        vector_ids: List[str], 
        namespace: Optional[str] = None,
        delete_all: bool = False
    ) -> Dict[str, Any]:
        """
        Delete vectors from the Pinecone index.
        
        :param vector_ids: List of vector IDs to delete.
        :param namespace: Optional namespace for the vectors.
        :param delete_all: Whether to delete all vectors (ignores vector_ids if True).
        :return: Response from Pinecone.
        """
        if delete_all:
            logger.info(f"Deleting ALL vectors from index {self.index_name}")
            try:
                kwargs = {"delete_all": True}
                if namespace:
                    kwargs["namespace"] = namespace
                    
                response = self.index.delete(**kwargs)
                logger.info("Successfully deleted all vectors")
                return response
            except Exception as error:
                logger.error(f"Failed to delete all vectors - Index: {self.index_name} | Error: {str(error)}")
                raise error
        else:
            logger.info(f"Deleting {len(vector_ids)} vectors from index {self.index_name}")
            try:
                kwargs = {"ids": vector_ids}
                if namespace:
                    kwargs["namespace"] = namespace
                    
                response = self.index.delete(**kwargs)
                logger.info("Successfully deleted vectors")
                return response
            except Exception as error:
                logger.error(f"Failed to delete vectors - Index: {self.index_name} | Error: {str(error)}")
                raise error

    def fetch_vectors(
        self, 
        vector_ids: List[str], 
        namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch specific vectors by ID from the Pinecone index.
        
        :param vector_ids: List of vector IDs to fetch.
        :param namespace: Optional namespace for the vectors.
        :return: Dictionary of vectors and their metadata.
        """
        logger.info(f"Fetching {len(vector_ids)} vectors from index {self.index_name}")
        try:
            kwargs = {"ids": vector_ids}
            if namespace:
                kwargs["namespace"] = namespace
                
            response = self.index.fetch(**kwargs)
            fetched_vectors = response.get("vectors", {})
            logger.info(f"Successfully fetched {len(fetched_vectors)} vectors")
            return response
        except Exception as error:
            logger.error(f"Failed to fetch vectors - Index: {self.index_name} | Error: {str(error)}")
            raise error

    def describe_index_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the index.
        
        :return: Index statistics.
        """
        logger.info(f"Getting statistics for index {self.index_name}")
        try:
            response = self.index.describe_index_stats()
            total_vector_count = response.get("total_vector_count", 0)
            logger.info(f"Index contains {total_vector_count} vectors")
            return response
        except Exception as error:
            logger.error(f"Failed to get index stats - Index: {self.index_name} | Error: {str(error)}")
            raise error