import json
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from typing import Optional, Dict, List, Any, Union

from ..logger import custom_logger

logger = custom_logger(__name__)

class DynamoDBHelper:
    """Custom helper for DynamoDB to simplify CRUD operations."""

    def __init__(
        self,
        table_name: str,
        pk_name: str,
        sk_name: Optional[str] = None,
        region_name: Optional[str] = None,
    ) -> None:
        """
        Initialize the DynamoDB helper.

        :param table_name: Name of the DynamoDB table.
        :param region_name: AWS region (optional, defaults to boto3 default).
        :param pk_name: Name of the partition key.
        :param sk_name: Name of the sort key (optional).
        """
        self.table_name = table_name
        self.pk_name = pk_name
        self.sk_name = sk_name
        self.dynamodb_client = boto3.client("dynamodb", region_name=region_name)  # Solo para operaciones específicas
        self.dynamodb_resource = boto3.resource("dynamodb", region_name=region_name)
        self.table = self.dynamodb_resource.Table(self.table_name)
        self._validate_table()
        logger.info(f"Configured helper for DynamoDB table: {table_name}")

    def _validate_table(self) -> None:
        """Validate that the table exists and is accessible"""
        try:
            self.dynamodb_client.describe_table(TableName=self.table_name)
        except ClientError as error:
            logger.error(f"Table {self.table_name} does not exist or is inaccessible")
            raise error

    def get_table(self):
        """
        Get the DynamoDB table object.
        
        :return: DynamoDB table object.
        """
        return self.table

    def get_item(
        self, partition_key: str, sort_key: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a single item from DynamoDB using the primary key (pk+sk).

        :param partition_key: Partition key value.
        :param sort_key: Sort key value (optional).
        :return: Item dictionary or None if not found.
        """
        key = {self.pk_name: partition_key}
        log_keys = f"PK: {partition_key}"
        if sort_key and self.sk_name:
            key[self.sk_name] = sort_key
            log_keys += f", SK: {sort_key}"

        logger.info(f"Retrieving item with {log_keys}")
        try:
            response = self.table.get_item(Key=key)
            item = response.get("Item")
            logger.info("Item retrieved successfully" if item else "Item not found")
            return item
        except ClientError as error:
            logger.error(
                f"Failed to retrieve item - Table: {self.table_name} | {log_keys} | "
                f"Error: {error.response['Error']['Code']}"
            )
            raise error

    def query_items_by_begins_pk_sk(
        self, partition_key: str, sort_key_portion: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Query items where PK and SK begin with the provided values.

        :param partition_key: Partition key prefix.
        :param sort_key_portion: Sort key prefix.
        :param limit: Maximum items per query (default: 50).
        :return: List of matching items.
        """
        logger.info(
            f"Querying items with PK begins_with: {partition_key}, "
            f"SK begins_with: {sort_key_portion}"
        )
        all_items = []
        try:
            key_condition = Key(self.pk_name).begins_with(partition_key) & Key(self.sk_name).begins_with(sort_key_portion)
            response = self.table.query(KeyConditionExpression=key_condition, Limit=limit)
            all_items.extend(response.get("Items", []))
            logger.debug(f"Initial query returned {len(response.get('Items', []))} items")

            while "LastEvaluatedKey" in response:
                response = self.table.query(
                    KeyConditionExpression=key_condition,
                    Limit=limit,
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                )
                all_items.extend(response.get("Items", []))
                logger.debug(f"Paged query returned {len(response.get('Items', []))} items")

            logger.info(f"Total items retrieved: {len(all_items)}")
            return all_items
        except ClientError as error:
            logger.error(
                f"Query failed - Table: {self.table_name} | PK: {partition_key} | "
                f"SK: {sort_key_portion} | Error: {error.response['Error']['Message']}"
            )
            raise error

    def put_item(self, data: Dict[str, Any], condition: Optional[str] = None) -> Dict[str, Any]:
        """
        Insert a new item into the DynamoDB table.

        :param data: Item to insert (simple Python dictionary).
        :param condition: Optional condition expression for the put operation.
        :return: Response from DynamoDB.
        """
        logger.info(f"Inserting item into table {self.table_name}")
        logger.debug(f"Data: {data}")
        try:
            kwargs = {"Item": data}
            if condition:
                kwargs["ConditionExpression"] = condition
            response = self.table.put_item(**kwargs)
            logger.info("Item inserted successfully")
            return response
        except ClientError as error:
            logger.error(
                f"Failed to insert item - Table: {self.table_name} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def update_item(
        self,
        partition_key: str,
        sort_key: Optional[str] = None,
        update_expression: str = None,
        expression_attribute_values: Optional[Dict[str, Any]] = None,
        condition_expression: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update an item in the DynamoDB table.

        :param partition_key: Partition key value.
        :param sort_key: Sort key value (optional).
        :param update_expression: Update expression (e.g., 'SET IS_DELETED = :val').
        :param expression_attribute_values: Values for the update expression.
        :param condition_expression: Optional condition for the update.
        :return: Response from DynamoDB.
        """
        key = {self.pk_name: partition_key}
        log_keys = f"{self.pk_name}: {partition_key}"
        if sort_key and self.sk_name:
            key[self.sk_name] = sort_key
            log_keys += f", {self.sk_name}: {sort_key}"

        logger.info(f"Updating item with {log_keys}")
        try:
            kwargs = {
                "Key": key,
                "UpdateExpression": update_expression,
                "ExpressionAttributeValues": expression_attribute_values,
                "ReturnValues": "UPDATED_NEW",
            }
            if condition_expression:
                kwargs["ConditionExpression"] = condition_expression

            response = self.table.update_item(**kwargs)
            logger.info("Item updated successfully")
            logger.debug(f"Updated attributes: {response.get('Attributes', {})}")
            return response
        except ClientError as error:
            logger.error(
                f"Failed to update item - Table: {self.table_name} | {log_keys} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def delete_item(
        self,
        partition_key: str,
        sort_key: Optional[str] = None,
        condition_expression: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Delete an item from the DynamoDB table.

        :param partition_key: Partition key value.
        :param sort_key: Sort key value (optional).
        :param condition_expression: Optional condition for the delete.
        :return: Response from DynamoDB.
        """
        key = {self.pk_name: partition_key}
        log_keys = f"{self.pk_name}: {partition_key}"
        if sort_key and self.sk_name:
            key[self.sk_name] = sort_key
            log_keys += f", {self.sk_name}: {sort_key}"

        logger.info(f"Deleting item with {log_keys}")
        try:
            kwargs = {"Key": key}
            if condition_expression:
                kwargs["ConditionExpression"] = condition_expression

            response = self.table.delete_item(**kwargs)
            logger.info("Item deleted successfully")
            return response
        except ClientError as error:
            logger.error(
                f"Failed to delete item - Table: {self.table_name} | {log_keys} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def batch_get_items(
        self, keys: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        """
        Retrieve multiple items from the DynamoDB table in a batch.

        :param keys: List of key dictionaries (e.g., [{"ALUMNO_ID": "val", "DATE_TIME": "val"}]).
        :return: List of retrieved items.
        """
        logger.info(f"Batch retrieving {len(keys)} items from table {self.table_name}")
        all_items = []
        try:
            while keys:
                batch = keys[:100]  # DynamoDB batch limit is 100
                response = self.dynamodb_resource.batch_get_item(
                    RequestItems={
                        self.table_name: {
                            "Keys": batch,
                            "ConsistentRead": False,
                        }
                    }
                )
                items = response.get("Responses", {}).get(self.table_name, [])
                all_items.extend(items)
                logger.debug(f"Batch retrieved {len(items)} items")

                # Handle unprocessed keys
                unprocessed = response.get("UnprocessedKeys", {}).get(self.table_name, {}).get("Keys", [])
                keys = unprocessed
                if unprocessed:
                    logger.warning(f"Retrying {len(unprocessed)} unprocessed keys")

            logger.info(f"Total items retrieved: {len(all_items)}")
            return all_items
        except ClientError as error:
            logger.error(
                f"Batch get failed - Table: {self.table_name} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def batch_write_items(
        self, put_items: Optional[List[Dict[str, Any]]] = None, delete_items: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        Write or delete multiple items in the DynamoDB table in a batch.

        :param put_items: List of items to put (simple Python dictionaries).
        :param delete_items: List of keys to delete (simple dictionaries).
        :return: Response from DynamoDB.
        """
        logger.info(f"Batch writing to table {self.table_name}")
        put_items = put_items or []
        delete_items = delete_items or []

        logger.debug(f"Processing {len(put_items)} puts and {len(delete_items)} deletes")
        try:
            with self.table.batch_writer() as batch:
                for item in put_items:
                    batch.put_item(Item=item)
                for key in delete_items:
                    batch.delete_item(Key=key)

            logger.info("Batch write completed successfully")
            return {}  # batch_writer doesn't return a standard response
        except ClientError as error:
            logger.error(
                f"Batch write failed - Table: {self.table_name} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def scan_table(self, filter_expression=None, expression_attribute_values=None, 
               expression_attribute_names=None, limit=None):
        """
        Escanea la tabla DynamoDB usando el recurso de alto nivel
        
        Args:
            filter_expression (str, optional): Expresión de filtro
            expression_attribute_values (dict, optional): Valores de atributos de expresión
            expression_attribute_names (dict, optional): Nombres de atributos de expresión
            limit (int, optional): Número máximo de elementos a retornar
            
        Returns:
            list: Lista de elementos que coinciden con el filtro
        """
        try:
            scan_params = {}
            
            if filter_expression:
                scan_params['FilterExpression'] = filter_expression
                
            if expression_attribute_values:
                scan_params['ExpressionAttributeValues'] = expression_attribute_values
                
            if expression_attribute_names:
                scan_params['ExpressionAttributeNames'] = expression_attribute_names
                
            if limit:
                scan_params['Limit'] = limit
                
            logger.info(f"Scanning table {self.table_name}")
            
            response = self.table.scan(**scan_params)
            items = response.get('Items', [])
            
            logger.info(f"Scan completed. Found {len(items)} items")
            
            return items
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"Scan failed - Table: {self.table_name} | Error: {error_code} | Message: {error_message}")
            raise e
        except Exception as e:
            logger.error(f"Error in scan operation: {str(e)}")
            raise e

    def query_table(self, key_condition, filter_expression=None, expression_attribute_values=None, 
                    expression_attribute_names=None, limit=None, scan_forward=True):
        """
        Realiza una operación de query en la tabla DynamoDB usando el recurso de alto nivel
        
        Args:
            key_condition (str): Expresión de condición de clave para la consulta
            filter_expression (str, optional): Expresión de filtro
            expression_attribute_values (dict, optional): Valores de atributos de expresión
            expression_attribute_names (dict, optional): Nombres de atributos de expresión
            limit (int, optional): Número máximo de elementos a retornar
            scan_forward (bool, optional): Si True, los resultados se ordenan ascendentemente por la clave de ordenamiento
            
        Returns:
            list: Lista de elementos que coinciden con la consulta
        """
        try:
            query_params = {
                'KeyConditionExpression': key_condition
            }
            
            if filter_expression:
                query_params['FilterExpression'] = filter_expression
                
            if expression_attribute_values:
                query_params['ExpressionAttributeValues'] = expression_attribute_values
                
            if expression_attribute_names:
                query_params['ExpressionAttributeNames'] = expression_attribute_names
                
            if limit:
                query_params['Limit'] = limit
                
            if not scan_forward:
                query_params['ScanIndexForward'] = False
                
            logger.info(f"Querying table {self.table_name}")
            
            response = self.table.query(**query_params)
            items = response.get('Items', [])
            
            logger.info(f"Query completed. Found {len(items)} items")
            
            return items
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"Query failed - Table: {self.table_name} | Error: {error_code} | Message: {error_message}")
            raise e
        except Exception as e:
            logger.error(f"Error in query operation: {str(e)}")
            raise e
    
    def query_by_index(
        self,
        index_name: str,
        key_condition_expression: Union[Key, str],
        filter_expression: Optional[Union[Attr, str]] = None,
        limit: int = 50,
        projection_expression: Optional[str] = None,
        expression_attribute_names: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query a secondary index in the DynamoDB table.

        :param index_name: Name of the secondary index.
        :param key_condition_expression: Key condition for the query (e.g., Key('index_key').eq('value')).
        :param filter_expression: Optional filter expression.
        :param limit: Maximum items per query (default: 50).
        :param projection_expression: Optional projection expression.
        :param expression_attribute_names: Optional attribute names for expressions.
        :return: List of matching items.
        """
        logger.info(f"Querying index {index_name} on table {self.table_name}")
        all_items = []
        try:
            kwargs = {
                "IndexName": index_name,
                "KeyConditionExpression": key_condition_expression,
                "Limit": limit,
            }
            if filter_expression:
                kwargs["FilterExpression"] = filter_expression
            if projection_expression:
                kwargs["ProjectionExpression"] = projection_expression
            if expression_attribute_names:
                kwargs["ExpressionAttributeNames"] = expression_attribute_names

            response = self.table.query(**kwargs)
            all_items.extend(response.get("Items", []))
            logger.debug(f"Initial query returned {len(response.get('Items', []))} items")

            while "LastEvaluatedKey" in response:
                kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
                response = self.table.query(**kwargs)
                all_items.extend(response.get("Items", []))
                logger.debug(f"Paged query returned {len(response.get('Items', []))} items")

            logger.info(f"Total items retrieved: {len(all_items)}")
            return all_items
        except ClientError as error:
            logger.error(
                f"Index query failed - Table: {self.table_name} | Index: {index_name} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error

    def transact_write_items(
        self,
        put_items: Optional[List[Dict[str, Any]]] = None,
        delete_items: Optional[List[Dict[str, Dict[str, str]]]] = None,
        update_items: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Perform a transactional write operation (put, delete, or update) on multiple items.

        :param put_items: List of items to put (DynamoDB attribute value format).
        :param delete_items: List of keys to delete.
        :param update_items: List of update operations (with Key, UpdateExpression, etc.).
        :return: Response from DynamoDB.
        """
        logger.info(f"Executing transactional write on table {self.table_name}")
        put_items = put_items or []
        delete_items = delete_items or []
        update_items = update_items or []
        transact_items = []

        for item in put_items:
            transact_items.append({"Put": {"TableName": self.table_name, "Item": item}})
        for key in delete_items:
            transact_items.append({"Delete": {"TableName": self.table_name, "Key": key}})
        for update in update_items:
            transact_items.append(
                {
                    "Update": {
                        "TableName": self.table_name,
                        "Key": update["Key"],
                        "UpdateExpression": update["UpdateExpression"],
                        "ExpressionAttributeValues": update.get("ExpressionAttributeValues"),
                    }
                }
            )

        logger.debug(f"Processing {len(transact_items)} transactional operations")
        try:
            response = self.dynamodb_client.transact_write_items(TransactItems=transact_items)
            logger.info("Transactional write completed successfully")
            return response
        except ClientError as error:
            logger.error(
                f"Transactional write failed - Table: {self.table_name} | "
                f"Error: {error.response['Error']['Code']} | "
                f"Message: {error.response['Error']['Message']}"
            )
            raise error