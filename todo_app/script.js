document.addEventListener('DOMContentLoaded', () => {
    const todoForm = document.getElementById('todo-form');
    const todoInput = document.getElementById('todo-input');
    const todoList = document.getElementById('todo-list');

    // Fetch and render initial tasks
    fetchTodos();

    // Add new task
    todoForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const title = todoInput.value.trim();
        
        if (!title) return;

        try {
            const response = await fetch('api.php', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ title })
            });
            const newTodo = await response.json();
            
            if (newTodo.id) {
                // Clear loading text if it's the first task
                if (todoList.querySelector('.loading-text')) {
                    todoList.innerHTML = '';
                }
                
                // Add to top of list
                const todoElement = createTodoElement(newTodo);
                todoList.prepend(todoElement);
                todoInput.value = '';
            }
        } catch (error) {
            console.error('Error adding todo:', error);
        }
    });

    // Fetch tasks from API
    async function fetchTodos() {
        try {
            const response = await fetch('api.php');
            const todos = await response.json();
            
            todoList.innerHTML = ''; // Clear loading text
            
            if (todos.length === 0) {
                todoList.innerHTML = '<div class="loading-text">No tasks yet. Add one above!</div>';
                return;
            }

            todos.forEach(todo => {
                const todoElement = createTodoElement(todo);
                todoList.appendChild(todoElement);
            });
        } catch (error) {
            console.error('Error fetching todos:', error);
            todoList.innerHTML = '<div class="loading-text" style="color: var(--danger);">Failed to load tasks. Check DB connection.</div>';
        }
    }

    // Create a DOM element for a task
    function createTodoElement(todo) {
        const li = document.createElement('li');
        li.className = `todo-item ${todo.is_completed == 1 ? 'completed' : ''}`;
        li.dataset.id = todo.id;

        const isChecked = todo.is_completed == 1 ? 'checked' : '';

        li.innerHTML = `
            <div class="checkbox-wrapper">
                <input type="checkbox" class="toggle-todo" ${isChecked}>
            </div>
            <span class="todo-text">${escapeHTML(todo.title)}</span>
            <button class="delete-btn" aria-label="Delete task"><i class="fas fa-trash-alt"></i></button>
        `;

        // Toggle Status Event
        const checkbox = li.querySelector('.toggle-todo');
        checkbox.addEventListener('change', async (e) => {
            const isCompleted = e.target.checked;
            
            if(isCompleted) {
                li.classList.add('completed');
            } else {
                li.classList.remove('completed');
            }

            try {
                await fetch('api.php', {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        id: todo.id,
                        is_completed: isCompleted
                    })
                });
            } catch (error) {
                console.error('Error updating todo:', error);
                // Revert UI on failure
                e.target.checked = !isCompleted;
                li.classList.toggle('completed');
            }
        });

        // Delete Task Event
        const deleteBtn = li.querySelector('.delete-btn');
        deleteBtn.addEventListener('click', async () => {
            // Animate removal
            li.style.opacity = '0';
            li.style.transform = 'translateX(-20px)';
            
            setTimeout(async () => {
                li.remove();
                
                // Show empty message if last item was deleted
                if (todoList.children.length === 0) {
                    todoList.innerHTML = '<div class="loading-text">No tasks yet. Add one above!</div>';
                }
            }, 300);

            try {
                await fetch(`api.php?id=${todo.id}`, { method: 'DELETE' });
            } catch (error) {
                console.error('Error deleting todo:', error);
                // Ideally handle UI reversion here if needed
            }
        });

        return li;
    }

    // Utility to prevent XSS
    function escapeHTML(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }
});
