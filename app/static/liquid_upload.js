async function sendData(url, data) {
  const formData  = new FormData();

  for(const name in data) {
    formData.append(name, data[name]);
  }

  const response = await fetch(url, {
    method: 'POST',
    body: formData
  });

  console.log(response);
  // ...
}
