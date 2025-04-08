import numpy as np

class DynamicBatchOptimizer:
    def __init__(self, config: dict):
        self.config = config
        self.current_size = config.get('initial_size', 500)
        self.max_size = config.get('max_size', 2000)
        self.min_size = config.get('min_size', 100)
        self.history = []

    def update(self, last_duration: float, memory_usage: float):
        current_mem_mb = memory_usage / 1e6
        self.history.append((last_duration, current_mem_mb))
        
        if current_mem_mb > (self.config['max_memory'] * self.config['memory_threshold']):
            new_size = max(self.min_size, int(self.current_size * self.config['reduce_factor']))
        
        elif len(self.history) > self.config['history_window']:
            avg_duration = np.mean([d for d, _ in self.history[-self.config['history_window']:]])
            
            if avg_duration > self.config['duration_threshold']:
                new_size = max(self.min_size, int(self.current_size * self.config['reduce_factor']))
            else:
                new_size = min(self.max_size, int(self.current_size * self.config['increase_factor']))
        
        self.current_size = new_size
        return self.current_size