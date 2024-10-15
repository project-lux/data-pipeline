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

        # Check the status of the checkboxes
        option1 = 'option1' in request.form
        option2 = 'option2' in request.form


        #call function to process URI
        result = process_uri(uri, option1, option2)

        #pass result to template
        return render_template('result.html', result=result)
    return render_template("index.html")

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=8080)
