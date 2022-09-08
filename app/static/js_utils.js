function convertDictToFormData(data) {
    const formData = new FormData();
    for (const name in data) {
        formData.append(name, data[name]);
    }
    return formData
}

