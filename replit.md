# PocketFlow Q&A Project

## Overview
A command-line question-answering application built with PocketFlow, a 100-line LLM framework. The app prompts users for questions and uses OpenAI's GPT-4o to generate answers.

## Current State
- **Status**: Ready to run (requires OpenAI API key)
- **Last Updated**: December 2025

## Project Architecture

### Directory Structure
```
.
├── main.py           # Entry point - runs the Q&A flow
├── flow.py           # Defines the PocketFlow workflow
├── nodes.py          # Node definitions (GetQuestionNode, AnswerNode)
├── utils/
│   └── call_llm.py   # OpenAI API wrapper
├── docs/
│   └── design.md     # Design documentation
├── assets/
│   └── banner.png    # Project banner
└── requirements.txt  # Python dependencies
```

### Key Components
- **PocketFlow**: Lightweight LLM framework for building node-based workflows
- **OpenAI**: GPT-4o model for answering questions
- **Flow**: GetQuestionNode (user input) -> AnswerNode (LLM response)

## Configuration

### Required Secrets
- `OPENAI_API_KEY`: Your OpenAI API key (required for the app to work)

### Running the App
The app runs as a console application. When started, it will:
1. Prompt for a question
2. Send the question to GPT-4o
3. Display the answer

## Dependencies
- pocketflow>=0.0.1
- openai

## User Preferences
(None recorded yet)

## Recent Changes
- December 2025: Initial import and setup for Replit environment
