<template>
  <div id="app">
    <img src="./assets/logo.png" />
    <router-view />
  </div>
</template>

<script>
import axios from "axios";

var config = {
  method: "get",
  url: "http://localhost:8090/hue/accounts/login",
  headers: {
    "Response-type": "application/json",
  },
};

axios(config)
  .then(function (response) {
    // console.log(response);
    var token = response.data["csrfmiddlewaretoken"];
    console.log(token);
    console.log(window.document.cookie)

    var FormData = require("form-data");
    var data = new FormData();
    // data.append("username", "sun_xo");
    // data.append("password", "sun_xo");
    data.append("username", "sunxo");
    data.append("password", "sunxo");
    data.append("next", "/");

    config = {
      method: "post",
      url: "http://localhost:8090/hue/accounts/login",
      headers: {
        "Response-type": "application/json",
        "X-CSRFToken": token,
      },
      data: data,
    };

    console.log(config);

    axios(config)
      .then(function (response) {
        console.log('csrftoken=' + response.data['csrftoken'] + '; sessionid=' + response.data['sessionid']);
      })
      .catch(function (error) {
        console.log(error);
      });
  })
  .catch(function (error) {
    console.log(error);
  });
</script>

<style>
#app {
  font-family: "Avenir", Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;

  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>
