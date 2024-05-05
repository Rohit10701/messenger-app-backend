import { z } from "zod";

const messageSchema = z.object({
    receiverID: z.string(),
    senderID: z.string(),
    message: z.string(),
  });

export default messageSchema;