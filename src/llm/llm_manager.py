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

# Send messages with custom temperature and max tokens
response2 = llm_manager.chat(messages, temperature=0.8, max_tokens=150)
print(response2.content)
"""

from langchain_groq import ChatGroq
from langchain_community.chat_models import ChatOpenAI
from config.env_loader import load_env
from langchain.schema import SystemMessage, HumanMessage, BaseMessage
from langchain.base_language import BaseLanguageModel

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

    def __init__(self, provider: str = "groq", model: str = "llama-3.1-8b-instant") -> None:
        self.provider: str = provider
        self.model: str = model

    def get_llm(
        self,
        temperature: float = 0.0,
        max_tokens: int | None = None
    ) -> BaseLanguageModel:
        """
        Instantiate and return a configured LLM instance.

        Parameters
        ----------
        temperature : float, default=0.0
            Sampling temperature for this call. Higher values yield more diverse outputs.
        max_tokens : int or None, optional
            Maximum number of tokens for the response.

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
            return ChatGroq(
                model_name=self.model,
                temperature=temperature,
                max_tokens=max_tokens
            )
        elif self.provider == "openai":
            return ChatOpenAI(
                model_name=self.model,
                temperature=temperature,
                max_tokens=max_tokens
            )
        else:
            raise ValueError(f"Provider '{self.provider}' is not supported.")

    def chat(
        self,
        messages: list[BaseMessage],
        temperature: float = 0.0,
        max_tokens: int | None = None
    ) -> BaseLanguageModel:
        """
        Send messages to the LLM with a specified temperature and optional parameters.

        Parameters
        ----------
        messages : list of SystemMessage/HumanMessage
            The messages to send to the LLM.
        temperature : float, default=0.0
            Sampling temperature for this call.
        max_tokens : int or None, optional
            Maximum number of tokens for the response.

        Returns
        -------
        response : BaseLanguageModel output
            The LLM response for the given messages.
        """
        llm: BaseLanguageModel = self.get_llm(
            temperature=temperature,
            max_tokens=max_tokens
        )
        return llm.invoke(messages)