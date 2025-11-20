import pandas as pd
import time
import requests
import os
import json
from dotenv import load_dotenv
from typing import Dict, Any, Optional, List, Union, Tuple
import logging
import backoff
import uuid
from token_minter import TokenMinter
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Load environment variables
SPACE_ID = os.environ.get("SPACE_ID")
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID")
CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET")

token_minter = TokenMinter(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    host=DATABRICKS_HOST
)


class GenieClient:
    def __init__(self, host: str, space_id: str):
        self.host = host
        self.space_id = space_id
        self.update_headers()
        
        self.base_url = f"https://{host}/api/2.0/genie/spaces/{space_id}"
    
    def update_headers(self) -> None:
        """Update headers with fresh token from token_minter"""
        self.headers = {
            "Authorization": f"Bearer {token_minter.get_token()}",
            "Content-Type": "application/json"
        }
    
    @backoff.on_exception(
        backoff.expo,
        Exception,  
        max_tries=5,
        factor=2,
        jitter=backoff.full_jitter,
        on_backoff=lambda details: logger.warning(
            f"API request failed. Retrying in {details['wait']:.2f} seconds (attempt {details['tries']})"
        )
    )
    def start_conversation(self, question: str) -> Dict[str, Any]:
        """Start a new conversation with the given question"""
        self.update_headers()  # Refresh token before API call
        url = f"{self.base_url}/start-conversation"
        payload = {"content": question}
        
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()
    
    @backoff.on_exception(
        backoff.expo,
        Exception,  # Retry on any exception
        max_tries=5,
        factor=2,
        jitter=backoff.full_jitter,
        on_backoff=lambda details: logger.warning(
            f"API request failed. Retrying in {details['wait']:.2f} seconds (attempt {details['tries']})"
        )
    )
    def send_message(self, conversation_id: str, message: str) -> Dict[str, Any]:
        """Send a follow-up message to an existing conversation"""
        self.update_headers()  # Refresh token before API call
        url = f"{self.base_url}/conversations/{conversation_id}/messages"
        payload = {"content": message}
        
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(
        backoff.expo,
        Exception,  # Retry on any exception
        max_tries=5,
        factor=2,
        jitter=backoff.full_jitter,
        on_backoff=lambda details: logger.warning(
            f"API request failed. Retrying in {details['wait']:.2f} seconds (attempt {details['tries']})"
        )
    )
    def get_message(self, conversation_id: str, message_id: str) -> Dict[str, Any]:
        """Get the details of a specific message"""
        self.update_headers()  # Refresh token before API call
        url = f"{self.base_url}/conversations/{conversation_id}/messages/{message_id}"
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(
        backoff.expo,
        Exception,  # Retry on any exception
        max_tries=5,
        factor=2,
        jitter=backoff.full_jitter,
        on_backoff=lambda details: logger.warning(
            f"API request failed. Retrying in {details['wait']:.2f} seconds (attempt {details['tries']})"
        )
    )
    def list_conversation_messages(self, conversation_id: str) -> Dict[str, Any]:
        """
        List all messages in a conversation with full details including suggested_questions.
        This endpoint returns more complete information than get_message.
        """
        self.update_headers()  # Refresh token before API call
        url = f"{self.base_url}/conversations/{conversation_id}/messages"
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(
        backoff.expo,
        Exception,  # Retry on any exception
        max_tries=5,
        factor=2,
        jitter=backoff.full_jitter,
        on_backoff=lambda details: logger.warning(
            f"API request failed. Retrying in {details['wait']:.2f} seconds (attempt {details['tries']})"
        )
    )
    def get_query_result(self, conversation_id: str, message_id: str, attachment_id: str) -> Dict[str, Any]:
        """Get the query result using the attachment_id endpoint"""
        self.update_headers()  # Refresh token before API call
        url = f"{self.base_url}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result"
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        result = response.json()
        
        # Extract data_array from the correct nested location
        data_array = []
        if 'statement_response' in result:
            if 'result' in result['statement_response']:
                data_array = result['statement_response']['result'].get('data_array', [])
            
        return {
                    'data_array': data_array,
                    'schema': result.get('statement_response', {}).get('manifest', {}).get('schema', {})
                }

    @backoff.on_exception(
        backoff.expo,
        Exception,  # Retry on any exception
        max_tries=5,
        factor=2,
        jitter=backoff.full_jitter,
        on_backoff=lambda details: logger.warning(
            f"API request failed. Retrying in {details['wait']:.2f} seconds (attempt {details['tries']})"
        )
    )
    def execute_query(self, conversation_id: str, message_id: str, attachment_id: str) -> Dict[str, Any]:
        """Execute a query using the attachment_id endpoint"""
        self.update_headers()  # Refresh token before API call
        url = f"{self.base_url}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/execute-query"
        
        response = requests.post(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=5,
        factor=2,
        jitter=backoff.full_jitter,
        on_backoff=lambda details: logger.warning(
            f"API request failed. Retrying in {details['wait']:.2f} seconds (attempt {details['tries']})"
        )
    )
    def get_space_details(self) -> Dict[str, Any]:
        """Get the Genie Space details including sample questions"""
        self.update_headers()  # Refresh token before API call
        url = f"https://{self.host}/api/2.0/genie/spaces/{self.space_id}"
        
        # Add query parameter to include serialized space data
        params = {"include_serialized_space": "true"}
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()
    
    def send_message_feedback(self, conversation_id: str, message_id: str, feedback_type: str) -> Dict[str, Any]:
        """
        Send feedback for a message using the Genie API.
        According to https://docs.databricks.com/api/workspace/genie/sendmessagefeedback
        
        Args:
            conversation_id: The conversation ID
            message_id: The message ID to provide feedback for
            feedback_type: Either "positive" or "negative"
            
        Returns:
            API response
        """
        self.update_headers()  # Refresh token before API call
        url = f"{self.base_url}/conversations/{conversation_id}/messages/{message_id}/feedback"
        
        # Correct format: {"rating": "POSITIVE"} or {"rating": "NEGATIVE"}
        rating_value = "POSITIVE" if feedback_type == "positive" else "NEGATIVE"
        payload = {"rating": rating_value}
        
        logger.info(f"Sending {feedback_type} feedback (rating={rating_value}) for message {message_id}")
        logger.debug(f"Request URL: {url}")
        logger.debug(f"Payload: {payload}")
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            logger.info(f"Feedback sent successfully: {response.status_code}")
            return response.json() if response.text else {}
        except requests.exceptions.HTTPError as e:
            # Log the actual error response to help debug
            logger.error(f"HTTP Error: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response body: {e.response.text}")
            raise

    def wait_for_message_completion(self, conversation_id: str, message_id: str, timeout: int = 300, poll_interval: int = 2) -> Dict[str, Any]:
        """
        Wait for a message to reach a terminal state (COMPLETED, ERROR, etc.).
        
        Args:
            conversation_id: The ID of the conversation
            message_id: The ID of the message
            timeout: Maximum time to wait in seconds
            poll_interval: Time between status checks in seconds
            
        Returns:
            The completed message
        """
        
        start_time = time.time()
        attempt = 1
        
        while time.time() - start_time < timeout:
            
            message = self.get_message(conversation_id, message_id)
            status = message.get("status")
            
            if status in ["COMPLETED", "ERROR", "FAILED"]:
                return message
                
            time.sleep(poll_interval)
            attempt += 1
            
        raise TimeoutError(f"Message processing timed out after {timeout} seconds")

def start_new_conversation(question: str) -> Tuple[str, Union[str, pd.DataFrame], Optional[str], List[str]]:
    """
    Start a new conversation with Genie.
    
    Args:
        question: The initial question
        
    Returns:
        Tuple containing:
        - conversation_id: The new conversation ID
        - response: Either text or DataFrame response
        - query_text: SQL query text if applicable, otherwise None
        - suggested_questions: List of suggested follow-up questions
    """
    
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=SPACE_ID
    )
    
    try:
        # Start a new conversation
        response = client.start_conversation(question)
        conversation_id = response.get("conversation_id")
        message_id = response.get("message_id")
        
        # Wait for the message to complete
        complete_message = client.wait_for_message_completion(conversation_id, message_id)
        
        # Fetch full message details including suggested_questions using list endpoint
        logger.info(f"Fetching full message details with suggested_questions")
        messages_response = client.list_conversation_messages(conversation_id)
        
        # Find the BOT's response message (not the user's message)
        # The message_id from start_conversation is the USER's message
        # We need to find the bot's response message which contains suggested_questions
        full_message = complete_message  # Default to what we have
        messages = messages_response.get("messages", [])
        logger.info(f"Found {len(messages)} messages in conversation")
        
        # Messages are typically ordered chronologically, so find the bot's response
        # after our user message
        bot_response_message = None
        for i, msg in enumerate(messages):
            logger.info(f"Message {i}: ID={msg.get('message_id')}, Role={msg.get('role', 'unknown')}")
            
            # Find our user message
            if msg.get("message_id") == message_id:
                logger.info(f"Found user message at index {i}")
                # Get the next message (bot's response)
                if i + 1 < len(messages):
                    bot_response_message = messages[i + 1]
                    logger.info(f"Found bot response at index {i+1}: {bot_response_message.get('message_id')}")
                break
        
        # Use bot's response if found, otherwise use the last message (most recent)
        if bot_response_message:
            full_message = bot_response_message
        elif messages:
            # Fallback: use the last message (should be bot's response)
            full_message = messages[-1]
            logger.info(f"Using last message in conversation: {full_message.get('message_id')}")
        
        # Process the response
        result, query_text, suggested_questions = process_genie_response(client, conversation_id, message_id, full_message)
        
        return conversation_id, result, query_text, suggested_questions
        
    except Exception as e:
        return None, f"Sorry, an error occurred: {str(e)}. Please try again.", None, []

def continue_conversation(conversation_id: str, question: str) -> Tuple[Union[str, pd.DataFrame], Optional[str], List[str]]:
    """
    Send a follow-up message in an existing conversation.
    
    Args:
        conversation_id: The existing conversation ID
        question: The follow-up question
        
    Returns:
        Tuple containing:
        - response: Either text or DataFrame response
        - query_text: SQL query text if applicable, otherwise None
        - suggested_questions: List of suggested follow-up questions
    """
    logger.info(f"Continuing conversation {conversation_id} with question: {question[:30]}...")
    
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=SPACE_ID
    )
    
    try:
        # Send follow-up message in existing conversation
        response = client.send_message(conversation_id, question)
        message_id = response.get("message_id")
        
        # Wait for the message to complete
        complete_message = client.wait_for_message_completion(conversation_id, message_id)
        
        # Fetch full message details including suggested_questions using list endpoint
        logger.info(f"Fetching full message details with suggested_questions")
        messages_response = client.list_conversation_messages(conversation_id)
        
        # Find the BOT's response message (not the user's message)
        # The message_id from send_message is the USER's message
        # We need to find the bot's response message which contains suggested_questions
        full_message = complete_message  # Default to what we have
        messages = messages_response.get("messages", [])
        logger.info(f"Found {len(messages)} messages in conversation")
        
        # Messages are typically ordered chronologically, so find the bot's response
        # after our user message
        bot_response_message = None
        for i, msg in enumerate(messages):
            logger.info(f"Message {i}: ID={msg.get('message_id')}, Role={msg.get('role', 'unknown')}")
            
            # Find our user message
            if msg.get("message_id") == message_id:
                logger.info(f"Found user message at index {i}")
                # Get the next message (bot's response)
                if i + 1 < len(messages):
                    bot_response_message = messages[i + 1]
                    logger.info(f"Found bot response at index {i+1}: {bot_response_message.get('message_id')}")
                break
        
        # Use bot's response if found, otherwise use the last message (most recent)
        if bot_response_message:
            full_message = bot_response_message
        elif messages:
            # Fallback: use the last message (should be bot's response)
            full_message = messages[-1]
            logger.info(f"Using last message in conversation: {full_message.get('message_id')}")
        
        # Process the response
        result, query_text, suggested_questions = process_genie_response(client, conversation_id, message_id, full_message)
        
        return result, query_text, suggested_questions
        
    except Exception as e:
        # Handle specific errors
        if "429" in str(e) or "Too Many Requests" in str(e):
            return "Sorry, the system is currently experiencing high demand. Please try again in a few moments.", None, []
        elif "Conversation not found" in str(e):
            return "Sorry, the previous conversation has expired. Please try your query again to start a new conversation.", None, []
        else:
            logger.error(f"Error continuing conversation: {str(e)}")
            return f"Sorry, an error occurred: {str(e)}", None, []

def process_genie_response(client, conversation_id, message_id, complete_message) -> Tuple[Union[str, pd.DataFrame], Optional[str], List[str]]:
    """
    Process the response from Genie
    
    Args:
        client: The GenieClient instance
        conversation_id: The conversation ID
        message_id: The message ID
        complete_message: The completed message response
        
    Returns:
        Tuple containing:
        - result: Either text or DataFrame response
        - query_text: SQL query text if applicable, otherwise None
        - suggested_questions: List of suggested follow-up questions
    """
    suggested_questions = []
    result = None
    query_text = None
    
    # Check attachments - FIRST pass: collect suggested questions from all attachments
    attachments = complete_message.get("attachments", [])
    logger.info(f"Processing {len(attachments)} attachments")
    
    for idx, attachment in enumerate(attachments):
        logger.info(f"Attachment {idx} keys: {list(attachment.keys())}")
        
        # Log the full attachment structure for debugging (without large content)
        debug_attachment = {k: v for k, v in attachment.items() if k not in ['text', 'query']}
        logger.info(f"Attachment {idx} structure: {json.dumps(debug_attachment, indent=2)}")
        
        # Extract suggested questions if present
        # According to https://docs.databricks.com/api/workspace/genie/listconversationmessages
        # Structure should be: attachment.suggested_questions.questions
        if "suggested_questions" in attachment:
            sq_data = attachment.get("suggested_questions")
            logger.info(f"Found suggested_questions field! Type: {type(sq_data)}, Content: {sq_data}")
            
            if isinstance(sq_data, dict) and "questions" in sq_data:
                suggested_questions = sq_data.get("questions", [])
                logger.info(f"✓ Found {len(suggested_questions)} suggested questions from Genie: {suggested_questions}")
            else:
                logger.warning(f"suggested_questions field exists but doesn't have expected structure: {sq_data}")
        else:
            logger.info(f"✗ No suggested_questions field in attachment {idx}")
    
    # SECOND pass: process content from attachments
    for attachment in attachments:
        attachment_id = attachment.get("attachment_id")
        
        # If there's text content in the attachment, use it
        if "text" in attachment and "content" in attachment["text"]:
            result = attachment["text"]["content"]
            query_text = None
            break
        
        # If there's a query, get the result
        elif "query" in attachment:
            query_text = attachment.get("query", {}).get("query", "")
            query_result = client.get_query_result(conversation_id, message_id, attachment_id)
           
            data_array = query_result.get('data_array', [])
            schema = query_result.get('schema', {})
            columns = [col.get('name') for col in schema.get('columns', [])]
            
            # If we have data, return as DataFrame
            if data_array:
                # If no columns from schema, create generic ones
                if not columns and data_array and len(data_array) > 0:
                    columns = [f"column_{i}" for i in range(len(data_array[0]))]
                
                result = pd.DataFrame(data_array, columns=columns)
                break
    
    # If we found result in attachments, return it
    if result is not None:
        logger.info(f"Returning result with {len(suggested_questions)} suggested questions")
        return result, query_text, suggested_questions
    
    # If no attachments or no data in attachments, return text content
    if 'content' in complete_message:
        logger.info(f"Returning content with {len(suggested_questions)} suggested questions")
        return complete_message.get('content', ''), None, suggested_questions
    
    logger.warning("No response available")
    return "No response available", None, []

def get_sample_questions() -> List[str]:
    """
    Fetch sample questions from the Genie Space.
    
    Returns:
        List of sample questions configured in the space
    """
    try:
        client = GenieClient(
            host=DATABRICKS_HOST,
            space_id=SPACE_ID
        )
        
        space_details = client.get_space_details()
        logger.info(f"Fetched space details from Genie Space")
        
        # Extract sample questions from serialized_space field according to Databricks API docs
        # https://docs.databricks.com/api/workspace/genie/getspace
        serialized_space = space_details.get('serialized_space')
        
        if not serialized_space:
            logger.warning("No serialized_space field found in response")
            return []
        
        # Parse the JSON string
        space_config = json.loads(serialized_space)
        
        # Navigate to config.sample_questions
        sample_questions_data = space_config.get('config', {}).get('sample_questions', [])
        
        # Extract question text from each sample question object
        # Format: [{"id": "...", "question": ["Question text"]}, ...]
        sample_questions = []
        for item in sample_questions_data:
            if isinstance(item, dict) and 'question' in item:
                question = item['question']
                # question is a list, take the first element
                if isinstance(question, list) and len(question) > 0:
                    sample_questions.append(question[0])
                elif isinstance(question, str):
                    sample_questions.append(question)
        
        logger.info(f"Retrieved {len(sample_questions)} sample questions from Genie Space")
        return sample_questions if sample_questions else []
        
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing serialized_space JSON: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Error fetching sample questions: {str(e)}")
        return []

def get_space_info() -> Tuple[Optional[str], Optional[str]]:
    """
    Fetch the Genie Space title and description.
    According to https://docs.databricks.com/api/workspace/genie/getspace
    
    Returns:
        Tuple containing:
        - space_title: The title of the space
        - space_description: The description of the space
    """
    try:
        client = GenieClient(
            host=DATABRICKS_HOST,
            space_id=SPACE_ID
        )
        
        space_details = client.get_space_details()
        logger.info(f"Fetched space details from Genie Space")
        
        # Log the full response for debugging (but not sensitive data)
        logger.info(f"Space details keys: {list(space_details.keys())}")
        
        # Extract title and description from the top-level response
        # According to the API docs, these should be at the root level
        space_title = space_details.get('title')
        space_description = space_details.get('description')
        
        # Fallback: try display_name or name if title is not present
        if not space_title:
            space_title = space_details.get('display_name') or space_details.get('name')
            logger.info(f"Title not found, using fallback: '{space_title}'")
        
        # Fallback: try to extract description from serialized_space if not at top level
        if not space_description:
            serialized_space = space_details.get('serialized_space')
            if serialized_space:
                try:
                    space_config = json.loads(serialized_space)
                    space_description = space_config.get('config', {}).get('description')
                    logger.info(f"Description not at top level, extracted from serialized_space")
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing serialized_space JSON: {str(e)}")
        
        logger.info(f"Retrieved space title: '{space_title}', description: '{space_description}'")
        return space_title, space_description
        
    except Exception as e:
        logger.error(f"Error fetching space info: {str(e)}")
        return None, None

def send_feedback(conversation_id: str, message_id: str, feedback_type: str) -> bool:
    """
    Send feedback for a message.
    
    Args:
        conversation_id: The conversation ID
        message_id: The message ID
        feedback_type: Either "positive" or "negative"
        
    Returns:
        True if successful, False otherwise
    """
    try:
        client = GenieClient(
            host=DATABRICKS_HOST,
            space_id=SPACE_ID
        )
        
        logger.info(f"Attempting to send {feedback_type} feedback...")
        logger.info(f"  Conversation ID: {conversation_id}")
        logger.info(f"  Message ID: {message_id}")
        
        result = client.send_message_feedback(conversation_id, message_id, feedback_type)
        logger.info(f"✓ Successfully sent {feedback_type} feedback for message {message_id}")
        logger.debug(f"API response: {result}")
        return True
        
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if hasattr(e, 'response') else 'unknown'
        logger.error(f"✗ HTTP Error {status_code} sending feedback: {str(e)}")
        
        # Provide helpful error messages
        if status_code == 400:
            logger.error("  → Bad Request: The payload format might be incorrect")
            logger.error("  → Note: The sendmessagefeedback API might not be available in your workspace")
        elif status_code == 401:
            logger.error("  → Unauthorized: Check your authentication token")
        elif status_code == 403:
            logger.error("  → Forbidden: You may not have permission to send feedback")
        elif status_code == 404:
            logger.error("  → Not Found: The conversation or message ID might be invalid")
        
        return False
        
    except Exception as e:
        logger.error(f"✗ Unexpected error sending feedback: {str(e)}")
        return False

def genie_query(question: str) -> Tuple[Union[str, pd.DataFrame], Optional[str], List[str], Optional[str], Optional[str]]:
    """
    Main entry point for querying Genie.
    
    Args:
        question: The question to ask
        
    Returns:
        Tuple containing:
        - response: Either text or DataFrame response
        - query_text: SQL query text if applicable, otherwise None
        - suggested_questions: List of suggested follow-up questions
        - conversation_id: The conversation ID (for feedback)
        - message_id: The message ID (for feedback)
    """
    try:
        # Start a new conversation for each query
        conversation_id, result, query_text, suggested_questions = start_new_conversation(question)
        
        # Get the bot's message ID from the conversation
        # We need to fetch the latest message to get the bot's response message_id
        client = GenieClient(
            host=DATABRICKS_HOST,
            space_id=SPACE_ID
        )
        messages_response = client.list_conversation_messages(conversation_id)
        messages = messages_response.get("messages", [])
        
        # Get the last message (bot's response)
        bot_message_id = None
        if messages:
            # Find the bot's response message (not the user's message)
            for msg in reversed(messages):
                if msg.get("role") != "USER":
                    bot_message_id = msg.get("message_id")
                    break
        
        return result, query_text, suggested_questions, conversation_id, bot_message_id
            
    except Exception as e:
        logger.error(f"Error in conversation: {str(e)}. Please try again.")
        return f"Sorry, an error occurred: {str(e)}. Please try again.", None, [], None, None
