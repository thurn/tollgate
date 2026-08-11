import * as Dialog from "@radix-ui/react-dialog";
import { AnimatePresence, motion } from "framer-motion";
import { X } from "lucide-react";
import type { ReactNode } from "react";
import { Button } from "./Button";

export function Modal({ open, onOpenChange, title, description, children, footer, width = "500px" }: { open: boolean; onOpenChange: (open: boolean) => void; title: string; description?: string; children: ReactNode; footer?: ReactNode; width?: string }) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <AnimatePresence>
        {open && <Dialog.Portal forceMount>
          <Dialog.Overlay asChild><motion.div className="modal__overlay" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} /></Dialog.Overlay>
          <Dialog.Content asChild aria-describedby={description ? undefined : undefined}>
            <motion.div className="modal" style={{ maxWidth: width }} initial={{ opacity: 0, scale: 0.97, y: 12 }} animate={{ opacity: 1, scale: 1, y: 0 }} exit={{ opacity: 0, scale: 0.98, y: 8 }} transition={{ type: "spring", stiffness: 420, damping: 34 }}>
              <div className="modal__header">
                <div><Dialog.Title>{title}</Dialog.Title>{description && <Dialog.Description>{description}</Dialog.Description>}</div>
                <Dialog.Close asChild><Button variant="ghost" size="icon" aria-label="Close dialog"><X size={17} /></Button></Dialog.Close>
              </div>
              <div className="modal__body">{children}</div>
              {footer && <div className="modal__footer">{footer}</div>}
            </motion.div>
          </Dialog.Content>
        </Dialog.Portal>}
      </AnimatePresence>
    </Dialog.Root>
  );
}

