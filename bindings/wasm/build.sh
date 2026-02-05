#!/bin/bash
set -e

echo "Building WASM bindings..."

# Check if wasm-pack is installed
if ! command -v wasm-pack &> /dev/null; then
    echo "wasm-pack is not installed. Please install it first:"
    echo "curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh"
    exit 1
fi

# Build the WASM module
echo "Building WASM module with wasm-pack..."
wasm-pack build --target web --out-dir pkg --release

# Create a simple test HTML file
cat > index.html << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Formualizer WASM Test</title>
</head>
<body>
    <h1>Formualizer WASM Test</h1>
    <input type="text" id="formula" value="=SUM(A1:B2)" style="width: 300px;">
    <button id="tokenize">Tokenize</button>
    <button id="parse">Parse</button>
    <pre id="output"></pre>
    
    <script type="module">
        import init, { tokenize, parse } from './pkg/formualizer_wasm.js';
        
        async function run() {
            await init();
            
            const output = document.getElementById('output');
            const formulaInput = document.getElementById('formula');
            
            document.getElementById('tokenize').addEventListener('click', () => {
                try {
                    const tokenizer = tokenize(formulaInput.value);
                    const tokens = tokenizer.tokens();
                    output.textContent = JSON.stringify(tokens, null, 2);
                } catch (e) {
                    output.textContent = `Error: ${e}`;
                }
            });
            
            document.getElementById('parse').addEventListener('click', () => {
                try {
                    const ast = parse(formulaInput.value);
                    const json = ast.toJSON();
                    output.textContent = JSON.stringify(json, null, 2);
                } catch (e) {
                    output.textContent = `Error: ${e}`;
                }
            });
        }
        
        run();
    </script>
</body>
</html>
EOF

echo "Build complete! You can test the bindings by:"
echo "1. Starting a local web server: python3 -m http.server 8000"
echo "2. Opening http://localhost:8000 in your browser"
