MATCH( msg : POST | COMMENT  {id: $messageId })- [:REPLYOF*0..3] -> (po : POST) <- [:CONTAINEROF] - (f : forum) - [:hasModerator] -> (mod : PERSON) return f.id as forumId, f.title as forumTitle, mod.id as moderatorId, mod.firstName as moderatorFirstName, mod.lastName as moderatorLastName