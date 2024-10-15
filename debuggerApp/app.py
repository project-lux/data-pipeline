from flask import Flask, render_template, request
from debugger import process_uri

app = Flask(__name__)
@app.route('/', methods=['GET','POST'])

def index():
    if request.method == "POST":
        #get uri from form
        uri = request.form['uri']
        if "view" in uri:
            uri = uri.replace("view","data")

        #call function to process URI
        result = process_uri(uri)

        #pass result to template
        return render_template('result.html', result=result)
    return render_template("index.html")

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=8080)
