const sh = require("shelljs");

sh.cp("node_modules/babel-to-go/babel-to-go.min.js", "demo/babel-to-go.bundle.min.js");
sh.exec("webpack");
