package dev.digitaldragon;

import static spark.Spark.get;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import spark.Request;
import spark.Response;
import spark.Route;


public class Main {
    public static void main(String[] args) {
        get("/urls/queue", (request, response) ->  {
            response.type("application/json");
            System.out.println(request.ip());

            String url = "https://wikipedia.org"; //todo url from queue

            return "{\"url\":\"" + url + "\"}"; //todo remove this badness
        });
    }
}