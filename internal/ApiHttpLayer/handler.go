package Handler

import (
	"io"
	"os"
	"fmt"
	"net/http"
)

func HandlePut(w http.ResponseWriter, r *http.Request){
    fmt.Println("Request received!") 

	r.ParseMultipartForm(10 << 20)

	file, handler, err := r.FormFile("FileName")
	if err != nil {
		http.Error(w, "Error retrieving the file", http.StatusBadRequest)
		return
	}
	defer file.Close()


	dst, err := os.Create("./uploads/" + handler.Filename)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	// Copy the uploaded file's content to the local destination file
	if _, err := io.Copy(dst, file); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Successfully uploaded: %s\n", handler.Filename)
}

//from handle the request will be stored directly into in memory storage

func HandleGet(w http.ResponseWriter, r *http.Request){
		fmt.Fprint(w, "Welcome to root page")
}


func HandleDelete(w http.ResponseWriter, r *http.Request){
		fmt.Fprint(w, "Delete request on root page")
}