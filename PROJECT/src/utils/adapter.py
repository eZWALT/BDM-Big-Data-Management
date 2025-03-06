from abc import ABC, abstractmethod

### Abstract class for format adapters, designed to decouple social media platforms from 
### different data formats. This structure ensures flexibility and scalability by allowing 
### multiple social media clients (n) and format adapters (m) to coexist without creating 
### a combinatorial explosion of combinations, keeping complexity at O(n + m).

class FormatAdapter(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    # Transforms the raw data into the desired format (e.g., video, text, image).
    def transform(self):
        pass

    @abstractmethod
    # Validates the raw data before transformation, ensuring it meets expected conditions.
    def validate(self):
        pass

    @abstractmethod
    # Extracts metadata (e.g., duration, resolution) from the raw data.
    def extract_metadata(self):
        pass
    
    @abstractmethod
    # Converts the data into the required format (if necessary).
    def convert(self):
        pass
