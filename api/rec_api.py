import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gradio as gr
import openai
from scripts.db import MovieDB
from scripts.prompts import USER_INPUT_TO_TAGS_SYSTEM_PROMPT, USER_INPUT_TO_TAGS_USER_PROMPT
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MovieRecommenderApp:
    def __init__(self):
        """Initialize the Vector Search application."""
        self.openai_client = openai.OpenAI()
        self.db = MovieDB(
            dbname=os.getenv("PG_DB", "mydb"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASS", "secret"),
            host=os.getenv("PG_HOST", "localhost"),
            port=int(os.getenv("PG_PORT", "5432"))
        )

    def generate_tags(self, text: str) -> List[str]:
        """Generate tags for the given text."""
        return self.openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": USER_INPUT_TO_TAGS_SYSTEM_PROMPT},
                {"role": "user", "content": USER_INPUT_TO_TAGS_USER_PROMPT.format(input=text)}
            ]
        ).choices[0].message.content.split(",")

    def search_by_tags(self, tags: List[str], limit: int = 5) -> List[Dict[str, Any]]:
        """Search for movies by tags."""
        return self.db.search_by_tags(tags, limit)

    def process_query(self, user_prompt: str) -> str:
        """Process the user query and return search results."""
        if not user_prompt.strip():
            return "❌ Please enter a search query."

        try:


            # Get embedding for the user prompt
            tags = self.generate_tags(user_prompt)
            print(tags)

            # Search for similar vectors
            search_results = self.search_by_tags(tags)

            # Format results
            if not search_results:
                return "No similar movies found."

            formatted_results = ["🔍 **Search Results:**\n"]
            for i, result in enumerate(search_results, 1):
                formatted_results.append(f"**{i}. {result[0]}**")

            return "\n".join(formatted_results)

        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            return f"❌ Error: {str(e)}"


def create_gradio_interface():
    """Create and return the Gradio interface."""
    app = MovieRecommenderApp()

    # Create the Gradio interface
    with gr.Blocks(title="Movie Recommendation System", theme=gr.themes.Soft()) as interface:
        gr.Markdown("# 🔍 Movie Recommendation System")
        gr.Markdown("Enter your search query to find matching movies.")

        with gr.Row():
            with gr.Column():
                # Input fields
                user_prompt = gr.Textbox(
                    label="Search Query",
                    placeholder="Enter your search query here...",
                    lines=2
                )

                search_btn = gr.Button("🔍 Search", variant="primary")

            with gr.Column():
                # Output
                results = gr.Markdown(
                    label="Search Results",
                    value="Enter a query and click Search to see results."
                )

        # Connect the search function
        search_btn.click(
            fn=app.process_query,
            inputs=[user_prompt],
            outputs=results
        )

        # Also trigger search on Enter in the prompt field
        user_prompt.submit(
            fn=app.process_query,
            inputs=[user_prompt],
            outputs=results
        )

        gr.Markdown("""
        ### 📝 Instructions:
        1. Share your movie preferences in the text box above
        2. Click **Search** to find similar movies
        3. The top 5 most similar movies will be displayed with similarity scores
        """)

    return interface


def main():
    """Main function to launch the Gradio interface."""
    interface = create_gradio_interface()
    interface.launch(
        share=False,
        server_name="0.0.0.0",
        server_port=7860
    )


if __name__ == "__main__":
    main()
