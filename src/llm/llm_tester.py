"""
LLM Chat Tester
===============

This script demonstrates how to use the LLMManager class to send
structured chat messages to a language model (LLM) and print the response.

It uses LangChain's SystemMessage and HumanMessage to define assistant
behavior and user input, and allows dynamic temperature per call.

Usage
-----
1. Ensure your environment variables are set in a `.env` file and loaded
   using `load_env()` (GROQ_API_KEY or OPENAI_API_KEY).
2. Run this script:

    python -m llm.llm_tester

Dependencies
------------
- llm.llm_manager (LLMManager)
- langchain.schema (SystemMessage, HumanMessage)
"""

from llm.llm_manager import LLMManager
from langchain.schema import SystemMessage, HumanMessage

def main():
    """
    Simple tester script for the LLMManager class.

    Demonstrates sending a prompt using structured chat messages
    and printing the response. Allows dynamic temperature per call.
    """
    # Instantiate the LLM manager (uses Groq + llama-3.1-8b-instant by default)
    llm_manager = LLMManager()

    # Define messages for chat
    messages = [
        SystemMessage(content="You are a helpful assistant."),
        HumanMessage(content="Imagine a futuristic city and describe it in three sentences.")
    ]

    # Send messages with default temperature
    response_default = llm_manager.chat(messages)
    print("Temperature 0.0")
    print("Prompt:", messages[-1].content)
    print("Response:", response_default.content)
    print("-" * 50)

    # Send messages with custom temperature
    response_hot = llm_manager.chat(messages, temperature=0.8)
    print("Temperature 0.8")
    print("Prompt:", messages[-1].content)
    print("Response:", response_hot.content)

if __name__ == "__main__":
    main()