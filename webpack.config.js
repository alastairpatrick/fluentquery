const BabiliPlugin = require("babili-webpack-plugin");
const path = require("path");
const webpack = require("webpack");

module.exports = {
  entry:{
    "demo": "./demo/demo.js",
  },
  output: {
    path: path.resolve(__dirname, "demo"),
    filename: "[name].bundle.js"
  },
  externals: {
    "babel-to-go": "Babel",
  },
  plugins: [
    new BabiliPlugin({}, {}),
  ]
};
