"""
Language Model Manager
======================

This module provides a class to configure and manage a language model (LLM)
using LangChain integrations.

It supports both Groq and OpenAI providers and automatically loads API keys
from environment variables using `load_env()`.

Dependencies
------------
- langchain_groq
- langchain_community.chat_models
- config.env_loader
- langchain.schema (SystemMessage, HumanMessage)

Example
-------
from config.llm_config import LLMManager
from langchain.schema import SystemMessage, HumanMessage

llm_manager = LLMManager()

messages = [
    SystemMessage(content="You are a helpful assistant."),
    HumanMessage(content="Explain machine learning in one sentence.")
]

# Send messages with default temperature
response = llm_manager.chat(messages)
print(response.content)

# Send messages with custom temperature
response2 = llm_manager.chat(messages, temperature=0.8)
print(response2.content)
"""

from langchain_groq import ChatGroq
from langchain_community.chat_models import ChatOpenAI
from config.env_loader import load_env
from langchain.schema import SystemMessage, HumanMessage

# Load environment variables from .env file
load_env()

class LLMManager:
    """
    Class to manage the instantiation and usage of a language model (LLM).

    Parameters
    ----------
    provider : str, default="groq"
        The provider of the language model. Options: "groq", "openai".
    model : str, default="llama-3.1-8b-instant"
        The name of the model to be used.
    """

    def __init__(self, provider="groq", model="llama-3.1-8b-instant"):
        self.provider = provider
        self.model = model

    def get_llm(self, temperature=0.0):
        """
        Instantiate and return a configured LLM instance.

        This method creates an instance of the selected language model provider
        with the specified temperature. It supports both Groq and OpenAI providers.

        Parameters
        ----------
        temperature : float, default=0.0
            Sampling temperature for this call. Higher values yield more diverse outputs.

        Returns
        -------
        BaseLanguageModel
            A configured instance of the language model compatible with LangChain.

        Raises
        ------
        ValueError
            If the specified provider is not supported.

        Notes
        -----
        Requires appropriate environment variables to be set:
        - GROQ_API_KEY for Groq
        - OPENAI_API_KEY for OpenAI
        """
        if self.provider == "groq":
            return ChatGroq(model_name=self.model, temperature=temperature)
        elif self.provider == "openai":
            return ChatOpenAI(model_name=self.model, temperature=temperature)
        else:
            raise ValueError(f"Provider '{self.provider}' is not supported.")

    def chat(self, messages, temperature=0.0):
        """
        Send messages to the LLM with a specified temperature.

        Parameters
        ----------
        messages : list of SystemMessage/HumanMessage
            The messages to send to the LLM.
        temperature : float, default=0.0
            Sampling temperature for this call.

        Returns
        -------
        response : BaseLanguageModel output
            The LLM response for the given messages.
        """
        llm = self.get_llm(temperature=temperature)
        return llm.invoke(messages)
