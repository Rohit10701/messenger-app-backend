import { createClient } from "@supabase/supabase-js";
import { SUPABASE_ANON_KEY, SUPABASE_URL } from "../config";


// creating supabase instance
const supabase = () => {
  let connection;
  try {
    connection = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
    console.log("connected to supabase");
  } catch (error) {
    console.log("Error ============");
    console.log(error);
  }
  return connection;
};

export default supabase;
