import os
import gradio as gr
import openai
from scripts.db import MovieDB
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VectorSearchApp:
    def __init__(self):
        """Initialize the Vector Search application."""
        self.openai_client = openai.OpenAI()
        self.db = MovieDB(
            dbname=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASS"),
            host=os.getenv("PG_HOST", "localhost"),
            port=int(os.getenv("PG_PORT", "5432"))
        )

    
    def generate_tags(self, text: str) -> List[str]:
        """Generate tags for the given text."""
        return self.openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a movie critic that watched and analysed thousands of movies."},
                {"role": "user", "content": text}
            ]
        )
    
    def search_by_tags(self, tags: List[str], limit: int = 5) -> List[Dict[str, Any]]:
        """Search for movies by tags."""
        return self.db.search_by_tags(tags, limit)
    
    def process_query(self, user_prompt: str, collection_name: str = "movies") -> str:
        """Process the user query and return search results."""
        if not user_prompt.strip():
            return "❌ Please enter a search query."
        
        try:
            # Update collection name if provided
            if collection_name.strip():
                self.collection_name = collection_name.strip()
            
            
            # Get embedding for the user prompt
            query_vector = self.get_embedding(user_prompt)
            
            # Search for similar vectors
            search_results = self.search_vectors(query_vector)
            
            # Format results
            if not search_results:
                return "No similar documents found."
            
            formatted_results = ["🔍 **Search Results:**\n"]
            for i, result in enumerate(search_results, 1):
                score = result["score"]
                payload = result["payload"]
                
                formatted_results.append(f"**{i}. Match (Score: {score:.4f})**")
                
                # Display payload content
                if isinstance(payload, dict):
                    for key, value in payload.items():
                        if key == "text" or key == "content":
                            # Truncate long text
                            text_preview = str(value)[:200] + "..." if len(str(value)) > 200 else str(value)
                            formatted_results.append(f"   {key}: {text_preview}")
                        else:
                            formatted_results.append(f"   {key}: {value}")
                else:
                    formatted_results.append(f"   Content: {payload}")
                
                formatted_results.append("")
            
            return "\n".join(formatted_results)
            
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            return f"❌ Error: {str(e)}"

def create_gradio_interface():
    """Create and return the Gradio interface."""
    app = VectorSearchApp()
    
    # Create the Gradio interface
    with gr.Blocks(title="Vector Search with OpenAI & Qdrant", theme=gr.themes.Soft()) as interface:
        gr.Markdown("# 🔍 Vector Search with OpenAI Embeddings & Qdrant")
        gr.Markdown("Enter your search query to find similar documents using semantic search.")
        
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
